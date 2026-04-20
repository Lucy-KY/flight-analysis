"""
Page 6 — Live NAS Status
=========================
Shows real-time FAA National Airspace System (NAS) delay programs
sourced from the daily FAA NAS ingestion pipeline.
"""

import math
import sys
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.db import run_query

st.set_page_config(page_title="Live NAS Status", page_icon="✈️", layout="wide")
st.title("✈️ Live NAS Status")
st.markdown(
    "Real-time FAA National Airspace System delay programs at major U.S. airports. "
    "Data is refreshed daily from the FAA NAS Status API."
)

# ── Check whether table has any data ──────────────────────────────────────────
try:
    count_df = run_query("SELECT COUNT(*) AS N FROM ANALYTICS.AIRPORT_NAS_STATUS")
    has_data = int(count_df["N"].iloc[0]) > 0
except Exception:
    has_data = False

if not has_data:
    st.info(
        "No NAS status data available yet. "
        "Data accumulates as the daily pipeline runs. "
        "Run `python pipeline/daily_pipeline.py` to populate."
    )
    st.stop()

# ── Determine latest fetch date ───────────────────────────────────────────────
try:
    latest_df = run_query("SELECT MAX(FETCH_DATE) AS LATEST FROM ANALYTICS.AIRPORT_NAS_STATUS")
    latest_date = latest_df["LATEST"].iloc[0]
except Exception:
    latest_date = None

if latest_date is None:
    st.warning("Could not determine latest fetch date.")
    st.stop()

# ── Section 1: KPI cards ──────────────────────────────────────────────────────
st.subheader(f"Current Status — {latest_date}")

kpi_df = run_query(f"""
    SELECT
        COUNT(DISTINCT IATA_CODE)                                         AS AIRPORTS_WITH_DELAY,
        SUM(CASE WHEN HAS_GROUND_STOP  THEN 1 ELSE 0 END)                AS TOTAL_GROUND_STOPS,
        SUM(CASE WHEN HAS_GROUND_DELAY THEN 1 ELSE 0 END)                AS TOTAL_GROUND_DELAYS,
        MAX(MAX_AVG_DELAY_MIN)                                            AS WORST_AVG_DELAY_MIN
    FROM ANALYTICS.AIRPORT_NAS_STATUS
    WHERE FETCH_DATE = '{latest_date}'
""")

if not kpi_df.empty:
    row = kpi_df.iloc[0]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Airports with Active Delays",  int(row["AIRPORTS_WITH_DELAY"] or 0))
    col2.metric("Total Ground Stops",           int(row["TOTAL_GROUND_STOPS"]  or 0))
    col3.metric("Total Ground Delays",          int(row["TOTAL_GROUND_DELAYS"] or 0))
    worst = row["WORST_AVG_DELAY_MIN"]
    col4.metric("Worst Avg Delay (min)",        f"{int(worst)} min" if (worst and not math.isnan(float(worst))) else "N/A")

st.divider()

# ── Section 2: Current delay programs table ───────────────────────────────────
st.subheader("Active Delay Programs")

try:
    detail_df = run_query(f"""
        SELECT
            n.IATA_CODE                 AS "Airport",
            n.DELAY_TYPE                AS "Type",
            n.AVG_DELAY_MIN             AS "Avg Delay (min)",
            n.TREND                     AS "Trend",
            n.REASON                    AS "Reason"
        FROM RAW.FAA_NAS_STATUS_RAW n
        WHERE n.FETCH_DATE = '{latest_date}'
          AND n.HAS_DELAY = TRUE
        ORDER BY n.AVG_DELAY_MIN DESC NULLS LAST, n.IATA_CODE
    """)
except Exception:
    detail_df = None

if detail_df is not None and not detail_df.empty:
    st.dataframe(detail_df, use_container_width=True)
else:
    st.success("No active delay programs for the latest fetch date.")

st.divider()

# ── Section 3: Historical trend — delay programs by date ─────────────────────
st.subheader("Delay Program Count by Date (Historical)")

hist_df = run_query("""
    SELECT
        FETCH_DATE,
        SUM(DELAY_PROGRAMS) AS TOTAL_PROGRAMS
    FROM ANALYTICS.AIRPORT_NAS_STATUS
    GROUP BY FETCH_DATE
    ORDER BY FETCH_DATE
""")

if not hist_df.empty:
    fig_hist = px.line(
        hist_df,
        x="FETCH_DATE",
        y="TOTAL_PROGRAMS",
        title="Total Active NAS Delay Programs per Day",
        labels={"FETCH_DATE": "Date", "TOTAL_PROGRAMS": "Total Delay Programs"},
        markers=True,
        color_discrete_sequence=["#636EFA"],
    )
    fig_hist.update_layout(hovermode="x unified")
    st.plotly_chart(fig_hist, use_container_width=True)
else:
    st.info("Not enough historical data to plot trend yet.")

st.divider()

# ── Section 4: Delay programs by type ─────────────────────────────────────────
st.subheader("Delay Programs by Type")

try:
    type_df = run_query(f"""
        SELECT
            DELAY_TYPE          AS "Delay Type",
            COUNT(*)            AS "Occurrences"
        FROM RAW.FAA_NAS_STATUS_RAW
        WHERE HAS_DELAY = TRUE
        GROUP BY DELAY_TYPE
        ORDER BY "Occurrences" DESC
    """)
except Exception:
    type_df = None

if type_df is not None and not type_df.empty:
    fig_type = px.bar(
        type_df,
        x="Delay Type",
        y="Occurrences",
        title="Delay Programs by Type (All History)",
        labels={"Delay Type": "Type", "Occurrences": "Count"},
        color="Delay Type",
        color_discrete_sequence=px.colors.qualitative.Plotly,
        text_auto=True,
    )
    fig_type.update_layout(showlegend=False)
    st.plotly_chart(fig_type, use_container_width=True)
else:
    st.info("No delay type breakdown data available yet.")

# ── Raw data expander ─────────────────────────────────────────────────────────
with st.expander("View raw NAS status records (latest date)"):
    try:
        raw_df = run_query(f"""
            SELECT *
            FROM RAW.FAA_NAS_STATUS_RAW
            WHERE FETCH_DATE = '{latest_date}'
            ORDER BY IATA_CODE, DELAY_TYPE
        """)
        st.dataframe(raw_df, use_container_width=True)
    except Exception as e:
        st.error(f"Could not load raw data: {e}")
