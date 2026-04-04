"""
Page 3 — Airport Statistics
=============================
Departure/arrival traffic volumes, delay rates, and comparisons
across U.S. airports, filterable by year and month.
"""

import sys
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.db import run_query

st.set_page_config(page_title="Airport Statistics", page_icon="🏢", layout="wide")
st.title("🏢 Airport Statistics")
st.markdown(
    "Analyze traffic volumes, departure delay rates, and arrival delay rates "
    "across U.S. airports."
)

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.header("Filters")

try:
    yr_df = run_query(
        "SELECT MIN(YEAR) AS MIN_Y, MAX(YEAR) AS MAX_Y FROM ANALYTICS.AIRPORT_STATS"
    )
    min_year = int(yr_df["MIN_Y"].iloc[0])
    max_year = int(yr_df["MAX_Y"].iloc[0])
except Exception:
    min_year, max_year = 2000, 2024

year_start, year_end = st.sidebar.slider(
    "Year range", min_year, max_year, (2019, max_year)
)
top_n = st.sidebar.slider("Show top N airports", 10, 50, 20)
min_flights = st.sidebar.number_input(
    "Min total departures (filter small airports)", value=10000, step=1000
)

# ── Load & aggregate ──────────────────────────────────────────────────────────
raw_df = run_query(f"""
    SELECT YEAR, MONTH, AIRPORT, CITY, STATE,
           DEP_FLIGHTS, ARR_FLIGHTS,
           DEP_DELAYED, ARR_DELAYED, DEP_CANCELLED,
           AVG_DEP_DELAY_MIN, AVG_ARR_DELAY_MIN,
           DEP_DELAY_RATE, ARR_DELAY_RATE
    FROM ANALYTICS.AIRPORT_STATS
    WHERE YEAR BETWEEN {year_start} AND {year_end}
""")

if raw_df.empty:
    st.warning("No airport data for the selected period.")
    st.stop()

agg = (
    raw_df.groupby(["AIRPORT", "CITY", "STATE"])
    .agg(
        DEP_FLIGHTS=("DEP_FLIGHTS", "sum"),
        ARR_FLIGHTS=("ARR_FLIGHTS", "sum"),
        DEP_DELAYED=("DEP_DELAYED", "sum"),
        ARR_DELAYED=("ARR_DELAYED", "sum"),
        DEP_CANCELLED=("DEP_CANCELLED", "sum"),
        AVG_DEP_DELAY_MIN=("AVG_DEP_DELAY_MIN", "mean"),
        AVG_ARR_DELAY_MIN=("AVG_ARR_DELAY_MIN", "mean"),
    )
    .reset_index()
)
agg["DEP_DELAY_RATE"] = (
    100.0 * agg["DEP_DELAYED"] / agg["DEP_FLIGHTS"].clip(lower=1)
).round(2)
agg["ARR_DELAY_RATE"] = (
    100.0 * agg["ARR_DELAYED"] / agg["ARR_FLIGHTS"].clip(lower=1)
).round(2)
agg["CANCEL_RATE"] = (
    100.0 * agg["DEP_CANCELLED"] / agg["DEP_FLIGHTS"].clip(lower=1)
).round(2)
agg["LABEL"] = agg["AIRPORT"] + " — " + agg["CITY"].str.split(",").str[0]

# Apply minimum flights filter
filtered = agg[agg["DEP_FLIGHTS"] >= min_flights].copy()

if filtered.empty:
    st.warning(f"No airports with ≥ {min_flights:,} departures in the selected period.")
    st.stop()

# ── Chart 1: Top N busiest airports by departures ─────────────────────────────
st.subheader(f"Top {top_n} Busiest Airports by Departures ({year_start}–{year_end})")

busiest = filtered.nlargest(top_n, "DEP_FLIGHTS").sort_values("DEP_FLIGHTS")
fig1 = px.bar(
    busiest, x="DEP_FLIGHTS", y="LABEL",
    orientation="h",
    color="DEP_DELAY_RATE",
    color_continuous_scale="RdYlGn_r",
    labels={"DEP_FLIGHTS": "Total Departures", "LABEL": "Airport", "DEP_DELAY_RATE": "Delay Rate (%)"},
    text_auto=",",
    title="Total Departures (colored by Delay Rate)",
)
fig1.update_layout(yaxis_title="", coloraxis_colorbar_title="Delay %")
st.plotly_chart(fig1, use_container_width=True)

# ── Chart 2: Top N highest departure delay rate airports ──────────────────────
st.subheader(f"Top {top_n} Highest Departure Delay Rate Airports")

most_delayed = filtered.nlargest(top_n, "DEP_DELAY_RATE").sort_values("DEP_DELAY_RATE")
fig2 = px.bar(
    most_delayed, x="DEP_DELAY_RATE", y="LABEL",
    orientation="h",
    color="DEP_DELAY_RATE",
    color_continuous_scale="OrRd",
    text_auto=".1f",
    labels={"DEP_DELAY_RATE": "Departure Delay Rate (%)", "LABEL": "Airport"},
    title=f"Highest Departure Delay Rate (%) — min {min_flights:,} flights",
)
fig2.update_layout(yaxis_title="", coloraxis_showscale=False)
st.plotly_chart(fig2, use_container_width=True)

# ── Chart 3: Departure vs Arrival delay rate scatter ─────────────────────────
st.subheader("Departure vs. Arrival Delay Rate Comparison")

top_scatter = filtered.nlargest(60, "DEP_FLIGHTS")
fig3 = px.scatter(
    top_scatter,
    x="DEP_DELAY_RATE",
    y="ARR_DELAY_RATE",
    size="DEP_FLIGHTS",
    color="STATE",
    hover_name="LABEL",
    text="AIRPORT",
    labels={
        "DEP_DELAY_RATE": "Dep Delay Rate (%)",
        "ARR_DELAY_RATE": "Arr Delay Rate (%)",
        "DEP_FLIGHTS": "Total Departures",
        "STATE": "State",
    },
    title="Departure vs. Arrival Delay Rate (top 60 airports by volume)",
)
# diagonal reference line
max_rate = max(top_scatter["DEP_DELAY_RATE"].max(), top_scatter["ARR_DELAY_RATE"].max())
fig3.add_shape(
    type="line", x0=0, y0=0, x1=max_rate, y1=max_rate,
    line=dict(color="gray", dash="dot", width=1),
)
fig3.add_annotation(
    x=max_rate * 0.85, y=max_rate * 0.9,
    text="Dep = Arr line", showarrow=False,
    font=dict(color="gray", size=10),
)
fig3.update_traces(textposition="top center", textfont_size=8)
st.plotly_chart(fig3, use_container_width=True)

# ── State-level heatmap ───────────────────────────────────────────────────────
st.subheader("Average Departure Delay Rate by State")

state_df = (
    filtered.groupby("STATE")
    .agg(
        DEP_FLIGHTS=("DEP_FLIGHTS", "sum"),
        DEP_DELAYED=("DEP_DELAYED", "sum"),
    )
    .reset_index()
)
state_df["DEP_DELAY_RATE"] = (
    100.0 * state_df["DEP_DELAYED"] / state_df["DEP_FLIGHTS"].clip(lower=1)
).round(2)

fig4 = px.choropleth(
    state_df,
    locations="STATE",
    locationmode="USA-states",
    color="DEP_DELAY_RATE",
    scope="usa",
    color_continuous_scale="RdYlGn_r",
    title="Departure Delay Rate (%) by State",
    labels={"DEP_DELAY_RATE": "Delay Rate (%)"},
)
fig4.update_layout(geo=dict(showlakes=True, lakecolor="lightblue"))
st.plotly_chart(fig4, use_container_width=True)

# ── Summary table ─────────────────────────────────────────────────────────────
with st.expander("View full airport data table"):
    display = (
        filtered[["AIRPORT", "CITY", "STATE", "DEP_FLIGHTS", "ARR_FLIGHTS",
                  "DEP_DELAY_RATE", "ARR_DELAY_RATE", "CANCEL_RATE",
                  "AVG_DEP_DELAY_MIN", "AVG_ARR_DELAY_MIN"]]
        .rename(columns={
            "AIRPORT": "Code",
            "CITY": "City",
            "STATE": "State",
            "DEP_FLIGHTS": "Dep Flights",
            "ARR_FLIGHTS": "Arr Flights",
            "DEP_DELAY_RATE": "Dep Delay %",
            "ARR_DELAY_RATE": "Arr Delay %",
            "CANCEL_RATE": "Cancel %",
            "AVG_DEP_DELAY_MIN": "Avg Dep Delay (min)",
            "AVG_ARR_DELAY_MIN": "Avg Arr Delay (min)",
        })
        .sort_values("Dep Flights", ascending=False)
        .reset_index(drop=True)
    )
    st.dataframe(
        display.style.format({
            "Dep Flights": "{:,.0f}",
            "Arr Flights": "{:,.0f}",
            "Dep Delay %": "{:.1f}",
            "Arr Delay %": "{:.1f}",
            "Cancel %": "{:.2f}",
            "Avg Dep Delay (min)": "{:.1f}",
            "Avg Arr Delay (min)": "{:.1f}",
        }).background_gradient(subset=["Dep Delay %"], cmap="RdYlGn_r"),
        use_container_width=True,
    )
