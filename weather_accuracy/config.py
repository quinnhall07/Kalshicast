#config.py
DB_PATH = "weather.db"

HEADERS = {
    "User-Agent": "weather_accuracy/1.0 (contact: quinn.hall.scho@gmail.com)"
}

STATIONS = [
    {
        "station_id": "KNYC",
        "name": "NYC Central Park",
        "lat": 40.78,
        "lon": -73.97,
    },
    {
        "station_id": "KMIA",
        "name": "Miami International Airport",
        "lat": 25.79,
        "lon": -80.32,
    },
    # add more...
]
SOURCES = {
    #National Weather Service
    "NWS": {
        "name": "National Weather Service",
        "enabled": True,
        "module": "collect_nws",
        "func": "fetch_nws_forecast",
    },
    # Open-Meteo base
    "OME_BASE": {
        "name": "Open-Meteo (default)",
        "enabled": True,
        "module": "collect_ome",
        "func": "fetch_ome_forecast",
        "params": {"model": "best"},
    },

    # Models as separate sources
    "OME_GFS":  {"name":"Open-Meteo GFS",   "enabled": True, "module":"collect_ome_model", "func":"fetch_openmeteo_model_forecast", "params":{"model":"gfs"}},
    "OME_EC":   {"name":"Open-Meteo ECMWF", "enabled": True, "module":"collect_ome_model", "func":"fetch_openmeteo_model_forecast", "params":{"model":"ecmwf"}},
    "OME_ICON": {"name":"Open-Meteo ICON",  "enabled": True, "module":"collect_ome_model", "func":"fetch_openmeteo_model_forecast", "params":{"model":"icon"}},
    "OME_GEM":  {"name":"Open-Meteo GEM",   "enabled": True, "module":"collect_ome_model", "func":"fetch_openmeteo_model_forecast", "params":{"model":"gem"}},

    #WeatherAPI
    "WAPI": {
        "name": "WeatherAPI",
        "enabled": True,  # flip to True when your scraper is stable
        "module": "collect_wapi",
        "func": "fetch_wapi_forecast",
    },
    #NOAA GFS
    "NGFS": {
        "name": "NOAA GFS",
        "enabled": False,  # flip to True when your scraper is stable
        "module": "collect_ngfs",
        "func": "fetch_ngfs_forecast",
    },
    #NOAA HRRR
    "NHR3": {
        "name": "NOAA HRRR",
        "enabled": False,  # flip to True when your scraper is stable
        "module": "collect_nhr3",
        "func": "fetch_nhr3_forecast",
    },
    #Visual Crossing
    "VCR": {
        "name": "Visual Crossing",
        "enabled": True,  # flip to True when your scraper is stable
        "module": "collect_vcr",
        "func": "fetch_vcr_forecast",
        "params": {
            "unitGroup": "us",
            "days": 2,
        },
    },
    #Tomorrow.io
    "TOM": {
        "name": "Tomorrow.io",
        "enabled": True,
        "module": "collect_tom",
        "func": "fetch_tom_forecast",
        "params": {
            "days": 2,        # today + tomorrow
            "units": "imperial",
        },
    },
}