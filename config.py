#config.py
DB_PATH = "weather.db"

HEADERS = {
    "User-Agent": "Kalshicast/1.0 (contact: quinn.hall.scho@gmail.com)"
}

# Forecast collection cadence (UTC hours)
FORECAST_CYCLE_UTC_HOURS = [0, 6, 12, 18]  # start with [0, 12] if you want 2x/day

# Lead-time anchor times (local station time)
TARGET_LOCAL_HOUR_HIGH = 15  # ~3pm local
TARGET_LOCAL_HOUR_LOW = 7    # ~7am local

STATIONS = [
    {
        "station_id": "KNYC",
        "cli_site": "NYC",
        "name": "NYC Central Park",
        "state": "NY",
        "timezone": "America/New_York",
        "lat": 40.78,
        "lon": -73.97,
        "elevation_ft": 154,
        "is_active": True,
    },
    {
        "station_id": "KMIA",
        "cli_site": "MIA",
        "name": "Miami International Airport",
        "state": "FL",
        "timezone": "America/New_York",
        "lat": 25.79,
        "lon": -80.32,
        "elevation_ft": 10,
        "is_active": True,
    },
    {
        "station_id": "KMSY",
        "cli_site": "MSY",
        "name": "New Orleans International Airport",
        "state": "LA",
        "timezone": "America/Chicago",
        "lat": 29.99,
        "lon": -90.25,
        "elevation_ft": 3,
        "is_active": True,
    },
    {
        "station_id": "KPHL",
        "cli_site": "PHL",
        "name": "Philadelphia International Airport",
        "state": "PA",
        "timezone": "America/New_York",
        "lat": 39.87,
        "lon": -75.23,
        "elevation_ft": 7,
        "is_active": True,
    },
    {
        "station_id": "KMDW",
        "cli_site": "MDW",
        "name": "Chicago Midway Airport",
        "state": "IL",
        "timezone": "America/Chicago",
        "lat": 41.78,
        "lon": -87.76,
        "elevation_ft": 617,
        "is_active": True,
    },
    {
        "station_id": "KLAX",
        "cli_site": "LAX",
        "name": "Los Angeles International Airport",
        "state": "CA",
        "timezone": "America/Los_Angeles",
        "lat": 33.93806,
        "lon": -118.38889,
        "elevation_ft": 125,
        "is_active": True,
    },
    {
        "station_id": "KAUS",
        "cli_site": "AUS",
        "name": "Austin-Bergstrom International Airport",
        "state": "TX",
        "timezone": "America/Chicago",
        "lat": 30.18,
        "lon": -97.68,
        "elevation_ft": 486,
        "is_active": True,
    },
    {
        "station_id": "KDEN",
        "cli_site": "DEN",
        "name": "Denver International Airport",
        "state": "CO",
        "timezone": "America/Denver",
        "lat": 39.85,
        "lon": -104.66,
        "elevation_ft": 5404,
        "is_active": True,
    },
    {
        "station_id": "KSEA",
        "cli_site": "SEA",
        "name": "Seattle-Tacoma International Airport",
        "state": "WA",
        "timezone": "America/Los_Angeles",
        "lat": 47.44472,
        "lon": -122.31361,
        "elevation_ft": 427,
        "is_active": True,
    },
    {
        "station_id": "KLAS",
        "cli_site": "LAS",
        "name": "Harry Reid International Airport",
        "state": "NV",
        "timezone": "America/Los_Angeles",
        "lat": 36.07188,
        "lon": -115.1634,
        "elevation_ft": 2180,
        "is_active": True,
    },
    {
        "station_id": "KSFO",
        "cli_site": "SFO",
        "name": "San Francisco International Airport",
        "state": "CA",
        "timezone": "America/Los_Angeles",
        "lat": 37.61961,
        "lon": -122.36558,
        "elevation_ft": 10,
        "is_active": True,
    },
    {
        "station_id": "KDCA",
        "cli_site": "DCA",
        "name": "Reagan National Airport",
        "state": "VA",
        "timezone": "America/New_York",
        "lat": 38.85,
        "lon": -77.03,
        "elevation_ft": 13,
        "is_active": True,
    },
    # add more...
]
SOURCES = {
    #National Weather Service
    "NWS": {
        "name": "National Weather Service",
        "enabled": True,
        "module": "collectors.collect_nws",
        "func": "fetch_nws_forecast",
    },
    # Open-Meteo base
    "OME_BASE": {
        "name": "Open-Meteo (default)",
        "enabled": True,
        "module": "collectors.collect_ome",
        "func": "fetch_ome_forecast",
        "params": {"model": "best"},
    },

    # Models as separate sources
    "OME_GFS":  {"name":"Open-Meteo GFS",   "enabled": True, "module":"collectors.collect_ome_model", "func":"fetch_ome_model_forecast", "params":{"model":"gfs"}},
    "OME_EC":   {"name":"Open-Meteo ECMWF", "enabled": True, "module":"collectors.collect_ome_model", "func":"fetch_ome_model_forecast", "params":{"model":"ecmwf"}},
    "OME_ICON": {"name":"Open-Meteo ICON",  "enabled": True, "module":"collectors.collect_ome_model", "func":"fetch_ome_model_forecast", "params":{"model":"icon"}},
    "OME_GEM":  {"name":"Open-Meteo GEM",   "enabled": True, "module":"collectors.collect_ome_model", "func":"fetch_ome_model_forecast", "params":{"model":"gem"}},

    #WeatherAPI
    "WAPI": {
        "name": "WeatherAPI",
        "enabled": True,  # flip to True when your scraper is stable
        "module": "collectors.collect_wapi",
        "func": "fetch_wapi_forecast",
    },
    #NOAA GFS
    "NGFS": {
        "name": "NOAA GFS",
        "enabled": False,  # flip to True when your scraper is stable
        "module": "collectors.collect_ngfs",
        "func": "fetch_ngfs_forecast",
    },
    #NOAA HRRR
    "NHR3": {
        "name": "NOAA HRRR",
        "enabled": False,  # flip to True when your scraper is stable
        "module": "collectors.collect_nhr3",
        "func": "fetch_nhr3_forecast",
    },
    #Visual Crossing
    "VCR": {
        "name": "Visual Crossing",
        "enabled": True,  # flip to True when your scraper is stable
        "module": "collectors.collect_vcr",
        "func": "fetch_vcr_forecast",
        "params": {
            "unitGroup": "us",
            "days": 4,
        },
    },
    #Tomorrow.io
    "TOM": {
        "name": "Tomorrow.io",
        "enabled": True,
        "module": "collectors.collect_tom",
        "func": "fetch_tom_forecast",
        "params": {
            "days": 4,        # today + tomorrow
            "units": "imperial",
        },
    },

}










