"""
Central registry for forecast source fetchers.

Config-driven:
- Reads SOURCES from config.py
- Dynamically imports each enabled source module and function
- Returns a dict: {source_id: callable}

Fetcher signature:
    fetcher(station: dict) -> list[dict]
Where each dict has:
    {"target_date": "YYYY-MM-DD", "high": float, "low": float}

Design goals:
- One broken source must NOT break the whole run
- Easy to enable/disable sources in config
- Optional per-source params passed to fetchers that support it
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Callable, Dict, List, Any

from config import SOURCES

ForecastRow = Dict[str, Any]
Fetcher = Callable[[dict], List[ForecastRow]]


@dataclass(frozen=True)
class LoadedSource:
    source_id: str
    fetcher: Fetcher
    module: str
    func: str


def _validate_cfg(source_id: str, cfg: dict) -> None:
    if "module" not in cfg or "func" not in cfg:
        raise ValueError(f"SOURCES['{source_id}'] must include 'module' and 'func'")
    if not isinstance(cfg["module"], str) or not isinstance(cfg["func"], str):
        raise ValueError(f"SOURCES['{source_id}']['module'/'func'] must be strings")
    if "params" in cfg and cfg["params"] is not None and not isinstance(cfg["params"], dict):
        raise ValueError(f"SOURCES['{source_id}']['params'] must be a dict if provided")


def _wrap_with_params(fn: Callable[..., Any], params: dict) -> Fetcher:
    """
    Return a fetcher(station) wrapper which tries fn(station, params) first,
    and falls back to fn(station) for collectors that don't accept params.

    This keeps all existing collectors compatible.
    """
    def _wrapped(station: dict):
        try:
            return fn(station, params)
        except TypeError:
            # Old collectors accept only (station)
            return fn(station)

    return _wrapped


def _load_one(source_id: str, cfg: dict) -> LoadedSource:
    _validate_cfg(source_id, cfg)
    mod_name = cfg["module"]
    fn_name = cfg["func"]
    params = cfg.get("params") or {}

    mod = importlib.import_module(mod_name)
    fn = getattr(mod, fn_name, None)
    if fn is None or not callable(fn):
        raise ImportError(f"{mod_name}.{fn_name} not found/callable for source '{source_id}'")

    fetcher: Fetcher = _wrap_with_params(fn, params) if params else fn  # type: ignore[assignment]

    return LoadedSource(source_id=source_id, fetcher=fetcher, module=mod_name, func=fn_name)


def load_fetchers(*, include_disabled: bool = False) -> Dict[str, Fetcher]:
    """
    Returns {source_id: fetcher} for enabled sources (or all if include_disabled=True).

    IMPORTANT: This will raise if config is invalid, but will NOT raise for individual
    source import failures if include_disabled is False and the source is disabled.
    """
    out: Dict[str, Fetcher] = {}

    for source_id, cfg in SOURCES.items():
        enabled = bool(cfg.get("enabled", False))
        if not enabled and not include_disabled:
            continue

        loaded = _load_one(source_id, cfg)
        out[source_id] = loaded.fetcher

    return out


def load_fetchers_safe() -> Dict[str, Fetcher]:
    """
    Like load_fetchers(), but never raises for per-source failures.
    It prints an error and skips that source.

    Use this in production runs so one broken source doesn't stop everything.
    """
    out: Dict[str, Fetcher] = {}

    for source_id, cfg in SOURCES.items():
        if not cfg.get("enabled", False):
            continue
        try:
            loaded = _load_one(source_id, cfg)
            out[source_id] = loaded.fetcher
        except Exception as e:
            print(f"[sources_registry] SKIP {source_id}: {type(e).__name__}: {e}")

    return out


def enabled_source_ids() -> List[str]:
    return [sid for sid, cfg in SOURCES.items() if cfg.get("enabled", False)]
