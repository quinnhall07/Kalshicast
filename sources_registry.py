# sources_registry.py
from __future__ import annotations

import importlib
from typing import Any, Callable, Dict

from config import SOURCES

Fetcher = Callable[[dict], Any]


def load_fetchers_safe() -> Dict[str, Fetcher]:
    out: Dict[str, Fetcher] = {}

    for source_id, spec in (SOURCES or {}).items():
        if not spec.get("enabled"):
            continue

        mod_name = spec["module"]
        fn_name = spec["func"]
        params = spec.get("params") or {}

        mod = importlib.import_module(mod_name)
        fn = getattr(mod, fn_name)

        if params:
            def _make_wrapped(f, p):
                return lambda station, f=f, p=p: f(station, p)
            out[source_id] = _make_wrapped(fn, params)
        else:
            out[source_id] = fn

    return out
