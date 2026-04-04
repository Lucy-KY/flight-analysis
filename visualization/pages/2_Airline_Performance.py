"""
Page 2 — Airline Performance
==============================
On-time rates, delay rates, and cancellation rates per airline,
filterable by year range.
"""

import sys
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.db import AIRLINE_NAMES, airline_label, run_query

st.set_page_config(page_title="Airline Performance", page_icon="✈️", layout="wide")
st.title("✈️ Airline Performance")
st.markdown(
    "Compare on-time performance, delay rates, and cancellation rates "
    "across U.S. carriers over any time range."
)

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.header("Filters")

try:
    yr_df = run_query(
        "SELECT MIN(YEAR) AS MIN_Y, MAX(YEAR) AS MAX_Y FROM ANALYTICS.AIRLINE_PERFORMANCE"
    )
    min_year = int(yr_df["MIN_Y"].iloc[0])
    max_year = int(yr_df["MAX_Y"].iloc[0])
except Exception:
    min_year, max_year = 2000, 2024

year_start, year_end = st.sidebar.slider(
    "Year range", min_year, max_year, (2015, max_year), step=1
)

top_n = st.sidebar.slider("Show top N airlines", 5, 30, 15)

# ── Load & aggregate ──────────────────────────────────────────────────────────
raw_df = run_query(f"""
    SELECT YEAR, MONTH, AIRLINE_CODE,
           TOTAL_FLIGHTS, DELAYED_FLIGHTS, CANCELLED_FLIGHTS, ON_TIME_FLIGHTS,
           DELAY_RATE, CANCEL_RATE, ON_TIME_RATE,
           AVG_DEP_DELAY_MIN, AVG_ARR_DELAY_MIN
    FROM ANALYTICS.AIRLINE_PERFORMANCE
    WHERE YEAR BETWEEN {year_start} AND {year_end}
""")

if raw_df.empty:
    st.warning("No airline data for the selected period.")
    st.stop()

agg = (
    raw_df.groupby("AIRLINE_CODE")
    .agg(
        TOTAL_FLIGHTS=("TOTAL_FLIGHTS", "sum"),
        DELAYED_FLIGHTS=("DELAYED_FLIGHTS", "sum"),
        CANCELLED_FLIGHTS=("CANCELLED_FLIGHTS", "sum"),
        ON_TIME_FLIGHTS=("ON_TIME_FLIGHTS", "sum"),
        AVG_DEP_DELAY_MIN=("AVG_DEP_DELAY_MIN", "mean"),
        AVG_ARR_DELAY_MIN=("AVG_ARR_DELAY_MIN", "mean"),
    )
    .reset_index()
)
agg["DELAY_RATE"] = (
    100.0 * agg["DELAYED_FLIGHTS"] / agg["TOTAL_FLIGHTS"]
).round(2)
agg["CANCEL_RATE"] = (
    100.0 * agg["CANCELLED_FLIGHTS"] / agg["TOTAL_FLIGHTS"]
).round(2)
agg["ON_TIME_RATE"] = (
    100.0 * agg["ON_TIME_FLIGHTS"] / agg["TOTAL_FLIGHTS"]
).round(2)
agg["AIRLINE_NAME"] = agg["AIRLINE_CODE"].apply(airline_label)

# Keep top_n by total flights
agg = agg.nlargest(top_n, "TOTAL_FLIGHTS")

# ── Chart 1: On-time rate horizontal bar ─────────────────────────────────────
st.subheader(f"On-Time Rate by Airline ({year_start}–{year_end})")

sorted_agg = agg.sort_values("ON_TIME_RATE", ascending=True)
fig1 = px.bar(
    sorted_agg, x="ON_TIME_RATE", y="AIRLINE_NAME",
    orientation="h",
    text_auto=".1f",
    color="ON_TIME_RATE",
    color_continuous_scale="RdYlGn",
    labels={"ON_TIME_RATE": "On-Time Rate (%)", "AIRLINE_NAME": "Airline"},
    title="On-Time Departure Rate (%) — higher is better",
)
fig1.update_layout(coloraxis_showscale=False, yaxis_title="")
st.plotly_chart(fig1, use_container_width=True)

# ── Chart 2: Delay rate vs avg delay scatter ──────────────────────────────────
st.subheader("Delay Rate vs. Average Delay Severity")

fig2 = px.scatter(
    agg,
    x="DELAY_RATE",
    y="AVG_DEP_DELAY_MIN",
    size="TOTAL_FLIGHTS",
    color="CANCEL_RATE",
    hover_name="AIRLINE_NAME",
    text="AIRLINE_CODE",
    color_continuous_scale="OrRd",
    labels={
        "DELAY_RATE": "Delay Rate (%)",
        "AVG_DEP_DELAY_MIN": "Avg Dep Delay (min)",
        "TOTAL_FLIGHTS": "Total Flights",
        "CANCEL_RATE": "Cancel Rate (%)",
    },
    title="Delay Rate vs. Avg Delay Minutes (bubble size = total flights)",
)
fig2.update_traces(textposition="top center", textfont_size=10)
fig2.update_layout(coloraxis_colorbar_title="Cancel %")
st.plotly_chart(fig2, use_container_width=True)

# ── Chart 3: Year-over-year delay rate trend for selected airlines ────────────
st.subheader("Year-over-Year Delay Rate Trend")

top_airlines = agg.nlargest(8, "TOTAL_FLIGHTS")["AIRLINE_CODE"].tolist()
yoy_df = run_query(f"""
    SELECT YEAR, AIRLINE_CODE,
           ROUND(100.0 * SUM(DELAYED_FLIGHTS) / SUM(TOTAL_FLIGHTS), 2) AS DELAY_RATE
    FROM ANALYTICS.AIRLINE_PERFORMANCE
    WHERE YEAR BETWEEN {year_start} AND {year_end}
      AND AIRLINE_CODE IN ({", ".join(f"'{a}'" for a in top_airlines)})
    GROUP BY YEAR, AIRLINE_CODE
    ORDER BY YEAR
""")

if not yoy_df.empty:
    yoy_df["AIRLINE_LABEL"] = yoy_df["AIRLINE_CODE"].apply(
        lambda c: AIRLINE_NAMES.get(c, c)
    )
    fig3 = px.line(
        yoy_df, x="YEAR", y="DELAY_RATE",
        color="AIRLINE_LABEL",
        markers=True,
        labels={"YEAR": "Year", "DELAY_RATE": "Delay Rate (%)", "AIRLINE_LABEL": "Airline"},
        title="Annual Delay Rate — Top 8 Airlines by Volume",
        color_discrete_sequence=px.colors.qualitative.Plotly,
    )
    fig3.update_layout(hovermode="x unified",
                       legend=dict(orientation="h", y=-0.25))
    st.plotly_chart(fig3, use_container_width=True)

# ── Summary table ─────────────────────────────────────────────────────────────
st.subheader("Summary Table")

display_cols = ["AIRLINE_NAME", "TOTAL_FLIGHTS", "ON_TIME_RATE",
                "DELAY_RATE", "CANCEL_RATE", "AVG_DEP_DELAY_MIN", "AVG_ARR_DELAY_MIN"]
display = (
    agg[display_cols]
    .rename(columns={
        "AIRLINE_NAME": "Airline",
        "TOTAL_FLIGHTS": "Total Flights",
        "ON_TIME_RATE": "On-Time Rate (%)",
        "DELAY_RATE": "Delay Rate (%)",
        "CANCEL_RATE": "Cancel Rate (%)",
        "AVG_DEP_DELAY_MIN": "Avg Dep Delay (min)",
        "AVG_ARR_DELAY_MIN": "Avg Arr Delay (min)",
    })
    .sort_values("On-Time Rate (%)", ascending=False)
    .reset_index(drop=True)
)
st.dataframe(
    display.style.format({
        "Total Flights": "{:,.0f}",
        "On-Time Rate (%)": "{:.1f}",
        "Delay Rate (%)": "{:.1f}",
        "Cancel Rate (%)": "{:.2f}",
        "Avg Dep Delay (min)": "{:.1f}",
        "Avg Arr Delay (min)": "{:.1f}",
    }).background_gradient(subset=["On-Time Rate (%)"], cmap="RdYlGn")
     .background_gradient(subset=["Delay Rate (%)"], cmap="RdYlGn_r"),
    use_container_width=True,
)
