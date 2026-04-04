"""
Snowflake query helper for Streamlit
=====================================
Provides a cached run_query() function so Streamlit pages
don't open/close a new connection on every rerun.

Usage:
    from utils.db import run_query
    df = run_query("SELECT * FROM ANALYTICS.DELAY_TRENDS_MONTHLY")
"""

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# ── Resolve project root & load credentials ───────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent   # .../code/
load_dotenv(BASE_DIR / ".env")
sys.path.insert(0, str(BASE_DIR))

from config.snowflake_conn import get_conn as _get_conn   # RSA key auth


# ── Cached query executor (TTL = 10 min) ─────────────────────────────────────
@st.cache_data(ttl=600, show_spinner="Querying Snowflake…")
def run_query(sql: str) -> pd.DataFrame:
    """
    Execute *sql* against FLIGHT_DB.ANALYTICS and return a DataFrame.
    Results are cached for 10 minutes; pass ttl= override if needed.
    """
    conn = _get_conn(schema="ANALYTICS")
    try:
        cur = conn.cursor()
        cur.execute("ALTER WAREHOUSE FLIGHT_WH RESUME IF SUSPENDED")
        cur.execute("USE DATABASE FLIGHT_DB")
        cur.execute(sql)
        return cur.fetch_pandas_all()
    finally:
        cur.close()
        conn.close()


# ── Convenience: check whether ANALYTICS tables are populated ─────────────────
@st.cache_data(ttl=120)
def analytics_ready() -> bool:
    try:
        df = run_query("SELECT COUNT(*) AS N FROM ANALYTICS.DELAY_TRENDS_MONTHLY")
        return int(df["N"].iloc[0]) > 0
    except Exception:
        return False


# ── IATA airline code → full name mapping ─────────────────────────────────────
AIRLINE_NAMES: dict[str, str] = {
    "AA": "American Airlines",
    "DL": "Delta Air Lines",
    "UA": "United Airlines",
    "WN": "Southwest Airlines",
    "B6": "JetBlue Airways",
    "AS": "Alaska Airlines",
    "NK": "Spirit Airlines",
    "F9": "Frontier Airlines",
    "G4": "Allegiant Air",
    "HA": "Hawaiian Airlines",
    "VX": "Virgin America",
    "OO": "SkyWest Airlines",
    "MQ": "Envoy Air",
    "YX": "Republic Airways",
    "9E": "Endeavor Air",
    "EV": "ExpressJet Airlines",
    "OH": "PSA Airlines",
    "YV": "Mesa Airlines",
    "US": "US Airways",
    "FL": "AirTran Airways",
    "CO": "Continental Airlines",
    "NW": "Northwest Airlines",
    "HP": "America West Airlines",
    "TW": "Trans World Airlines (TWA)",
    "TZ": "ATA Airlines",
    "XE": "ExpressJet (XE)",
    "ZW": "Air Wisconsin",
    "CP": "Compass Airlines",
    "PT": "Piedmont Airlines",
}


def airline_label(code: str) -> str:
    """Return 'Full Name (CODE)' or just 'CODE' if unknown."""
    name = AIRLINE_NAMES.get(str(code), "")
    return f"{name} ({code})" if name else str(code)
