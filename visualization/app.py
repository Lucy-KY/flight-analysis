"""
U.S. Flight Delay & Traffic Pattern Analysis — Dashboard Home
=============================================================
Streamlit entry point.  Run with:
    cd code/
    streamlit run visualization/app.py

Navigation is handled by Streamlit's native multipage feature
(files in visualization/pages/ appear automatically in the sidebar).
"""

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st

# Make utils importable regardless of working directory
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils.db import analytics_ready, run_query

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="U.S. Flight Delay Analysis",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("✈️ Flight Analytics")
st.sidebar.markdown(
    """
**CSE-5114 Final Project**
Yiyang Sun · Kaiyuan Xu
Washington University in St. Louis

---
**Data Sources**
- BTS On-Time Performance (2000–2025)
- FAA NAS Status (daily)

**Warehouse**
Snowflake · FLIGHT_DB
"""
)

# ── Main header ───────────────────────────────────────────────────────────────
st.title("✈️ U.S. Flight Delay & Traffic Pattern Analysis")
st.markdown(
    "Interactive dashboard powered by **25 years** of BTS historical data "
    "and daily FAA NAS live status feeds."
)

# ── Readiness check ───────────────────────────────────────────────────────────
if not analytics_ready():
    st.warning(
        "⚠️ ANALYTICS tables appear to be empty. "
        "Run `python analysis/run_analytics.py` first to populate them."
    )
    st.stop()

# ── KPI cards ─────────────────────────────────────────────────────────────────
st.subheader("📊 Overall Statistics")

try:
    kpi_df = run_query("""
        SELECT
            SUM(TOTAL_FLIGHTS)                                         AS TOTAL_FLIGHTS,
            ROUND(AVG(DELAY_RATE), 2)                                  AS AVG_DELAY_RATE,
            ROUND(AVG(AVG_DEP_DELAY_MIN), 2)                           AS AVG_DEP_DELAY_MIN,
            ROUND(AVG(AVG_ARR_DELAY_MIN), 2)                           AS AVG_ARR_DELAY_MIN,
            MIN(YEAR) || ' – ' || MAX(YEAR)                            AS YEAR_RANGE,
            COUNT(DISTINCT YEAR)                                       AS YEARS_COVERED
        FROM ANALYTICS.DELAY_TRENDS_MONTHLY
    """)

    airline_count_df = run_query("""
        SELECT COUNT(DISTINCT AIRLINE_CODE) AS N_AIRLINES
        FROM ANALYTICS.AIRLINE_PERFORMANCE
    """)

    row = kpi_df.iloc[0]
    n_airlines = int(airline_count_df["N_AIRLINES"].iloc[0])

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Flights", f"{int(row['TOTAL_FLIGHTS']):,}")
    col2.metric("Avg Delay Rate", f"{row['AVG_DELAY_RATE']:.1f}%")
    col3.metric("Avg Dep Delay", f"{row['AVG_DEP_DELAY_MIN']:.1f} min")
    col4.metric("Avg Arr Delay", f"{row['AVG_ARR_DELAY_MIN']:.1f} min")
    col5.metric("Airlines Tracked", f"{n_airlines}  |  {row['YEAR_RANGE']}")

except Exception as e:
    st.error(f"Could not load KPI metrics: {e}")

st.divider()

# ── Yearly summary charts ─────────────────────────────────────────────────────
st.subheader("📈 Annual Trends (All Years)")

try:
    yearly_df = run_query("""
        SELECT
            YEAR,
            SUM(TOTAL_FLIGHTS)                                          AS TOTAL_FLIGHTS,
            SUM(TOTAL_DELAYED)                                          AS TOTAL_DELAYED,
            SUM(TOTAL_CANCELLED)                                        AS TOTAL_CANCELLED,
            ROUND(100.0 * SUM(TOTAL_DELAYED) / SUM(TOTAL_FLIGHTS), 2)  AS DELAY_RATE,
            ROUND(AVG(AVG_DEP_DELAY_MIN), 2)                           AS AVG_DEP_DELAY_MIN
        FROM ANALYTICS.DELAY_TRENDS_MONTHLY
        GROUP BY YEAR
        ORDER BY YEAR
    """)

    col_l, col_r = st.columns(2)

    with col_l:
        fig1 = px.line(
            yearly_df,
            x="YEAR",
            y="DELAY_RATE",
            markers=True,
            title="Annual Departure Delay Rate (%)",
            labels={"YEAR": "Year", "DELAY_RATE": "Delay Rate (%)"},
            color_discrete_sequence=["#EF553B"],
        )
        fig1.update_layout(hovermode="x unified")
        st.plotly_chart(fig1, use_container_width=True)

    with col_r:
        fig2 = px.bar(
            yearly_df,
            x="YEAR",
            y="TOTAL_FLIGHTS",
            title="Annual Total Flights",
            labels={"YEAR": "Year", "TOTAL_FLIGHTS": "Flights"},
            color_discrete_sequence=["#636EFA"],
        )
        st.plotly_chart(fig2, use_container_width=True)

except Exception as e:
    st.error(f"Could not load annual trends: {e}")

st.divider()

# ── Seasonal pattern (avg delay rate by month across all years) ───────────────
st.subheader("🗓️ Seasonal Delay Pattern")

try:
    seasonal_df = run_query("""
        SELECT
            MONTH,
            ROUND(AVG(DELAY_RATE), 2)       AS AVG_DELAY_RATE,
            ROUND(AVG(AVG_DEP_DELAY_MIN), 2) AS AVG_DEP_DELAY_MIN
        FROM ANALYTICS.DELAY_TRENDS_MONTHLY
        GROUP BY MONTH
        ORDER BY MONTH
    """)
    month_labels = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
    }
    seasonal_df["MONTH_LABEL"] = seasonal_df["MONTH"].map(month_labels)

    fig3 = px.bar(
        seasonal_df,
        x="MONTH_LABEL",
        y="AVG_DELAY_RATE",
        title="Average Delay Rate by Month (All Years)",
        labels={"MONTH_LABEL": "Month", "AVG_DELAY_RATE": "Avg Delay Rate (%)"},
        color="AVG_DELAY_RATE",
        color_continuous_scale="RdYlGn_r",
        text_auto=".1f",
    )
    fig3.update_layout(coloraxis_showscale=False, showlegend=False)
    st.plotly_chart(fig3, use_container_width=True)

except Exception as e:
    st.error(f"Could not load seasonal data: {e}")

st.caption(
    "Data: BTS On-Time Performance (2000–2025) + FAA NAS Status. "
    "Dashboard: CSE-5114 Final Project, Spring 2026."
)
