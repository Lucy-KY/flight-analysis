"""
Page 5 — Route Performance
============================
Identify high-delay and high-volume routes, with distance vs delay
scatter and a searchable full route table.
"""

import sys
from pathlib import Path

import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.db import run_query

st.set_page_config(page_title="Route Performance", page_icon="🗺️", layout="wide")
st.title("🗺️ Route Performance")
st.markdown(
    "Discover which routes have the worst delay rates and explore the "
    "relationship between route distance and on-time performance."
)

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.header("Filters")

try:
    yr_df = run_query(
        "SELECT MIN(YEAR) AS MIN_Y, MAX(YEAR) AS MAX_Y FROM ANALYTICS.ROUTE_PERFORMANCE"
    )
    min_year = int(yr_df["MIN_Y"].iloc[0])
    max_year = int(yr_df["MAX_Y"].iloc[0])
except Exception:
    min_year, max_year = 2000, 2024

year_start, year_end = st.sidebar.slider(
    "Year range", min_year, max_year, (2019, max_year)
)
top_n = st.sidebar.slider("Show top N routes", 10, 50, 20)
min_flights = st.sidebar.number_input(
    "Min total flights on route", value=1000, step=500
)

# ── Load & aggregate ──────────────────────────────────────────────────────────
raw_df = run_query(f"""
    SELECT YEAR, MONTH, ORIGIN, DEST, ROUTE,
           TOTAL_FLIGHTS, DELAYED_FLIGHTS, DELAY_RATE,
           AVG_DEP_DELAY_MIN, AVG_DISTANCE
    FROM ANALYTICS.ROUTE_PERFORMANCE
    WHERE YEAR BETWEEN {year_start} AND {year_end}
""")

if raw_df.empty:
    st.warning("No route data for the selected period.")
    st.stop()

agg = (
    raw_df.groupby(["ORIGIN", "DEST", "ROUTE"])
    .agg(
        TOTAL_FLIGHTS=("TOTAL_FLIGHTS", "sum"),
        DELAYED_FLIGHTS=("DELAYED_FLIGHTS", "sum"),
        AVG_DEP_DELAY_MIN=("AVG_DEP_DELAY_MIN", "mean"),
        AVG_DISTANCE=("AVG_DISTANCE", "mean"),
    )
    .reset_index()
)
agg["DELAY_RATE"] = (
    100.0 * agg["DELAYED_FLIGHTS"] / agg["TOTAL_FLIGHTS"].clip(lower=1)
).round(2)
agg["AVG_DEP_DELAY_MIN"] = agg["AVG_DEP_DELAY_MIN"].round(1)
agg["AVG_DISTANCE"] = agg["AVG_DISTANCE"].round(0)

filtered = agg[agg["TOTAL_FLIGHTS"] >= min_flights].copy()

if filtered.empty:
    st.warning(f"No routes with ≥ {min_flights:,} flights in the selected period.")
    st.stop()

# ── Chart 1: Most delayed routes (horizontal bar) ────────────────────────────
st.subheader(f"Top {top_n} Most Delayed Routes ({year_start}–{year_end})")

most_delayed = filtered.nlargest(top_n, "DELAY_RATE").sort_values("DELAY_RATE")
fig1 = px.bar(
    most_delayed,
    x="DELAY_RATE",
    y="ROUTE",
    orientation="h",
    text_auto=".1f",
    color="AVG_DEP_DELAY_MIN",
    color_continuous_scale="OrRd",
    labels={
        "DELAY_RATE": "Delay Rate (%)",
        "ROUTE": "Route",
        "AVG_DEP_DELAY_MIN": "Avg Delay (min)",
    },
    hover_data={"TOTAL_FLIGHTS": ":,", "AVG_DISTANCE": ":.0f mi"},
    title=f"Highest Delay Rate Routes (min {min_flights:,} flights)",
)
fig1.update_layout(yaxis_title="", coloraxis_colorbar_title="Avg Delay (min)")
st.plotly_chart(fig1, use_container_width=True)

# ── Chart 2: Highest volume routes ───────────────────────────────────────────
st.subheader(f"Top {top_n} Busiest Routes by Flights")

busiest = filtered.nlargest(top_n, "TOTAL_FLIGHTS").sort_values("TOTAL_FLIGHTS")
fig2 = px.bar(
    busiest,
    x="TOTAL_FLIGHTS",
    y="ROUTE",
    orientation="h",
    text_auto=",",
    color="DELAY_RATE",
    color_continuous_scale="RdYlGn_r",
    labels={
        "TOTAL_FLIGHTS": "Total Flights",
        "ROUTE": "Route",
        "DELAY_RATE": "Delay Rate (%)",
    },
    title="Busiest Routes (colored by Delay Rate)",
)
fig2.update_layout(yaxis_title="", coloraxis_colorbar_title="Delay %")
st.plotly_chart(fig2, use_container_width=True)

# ── Chart 3: Distance vs Delay Rate scatter ───────────────────────────────────
st.subheader("Route Distance vs. Delay Rate")

top_scatter = filtered.nlargest(200, "TOTAL_FLIGHTS")
fig3 = px.scatter(
    top_scatter,
    x="AVG_DISTANCE",
    y="DELAY_RATE",
    size="TOTAL_FLIGHTS",
    color="AVG_DEP_DELAY_MIN",
    hover_name="ROUTE",
    color_continuous_scale="OrRd",
    labels={
        "AVG_DISTANCE": "Avg Route Distance (miles)",
        "DELAY_RATE": "Delay Rate (%)",
        "TOTAL_FLIGHTS": "Total Flights",
        "AVG_DEP_DELAY_MIN": "Avg Delay (min)",
    },
    title="Distance vs. Delay Rate (top 200 routes by volume; bubble = flights)",
    opacity=0.7,
)
fig3.update_layout(coloraxis_colorbar_title="Avg Delay (min)")
st.plotly_chart(fig3, use_container_width=True)

# ── Chart 4: Year-over-year delay for a specific route ────────────────────────
st.subheader("Year-over-Year Trend for a Specific Route")

all_routes = sorted(filtered["ROUTE"].unique().tolist())
default_route = "LAX-JFK" if "LAX-JFK" in all_routes else all_routes[0]
selected_route = st.selectbox("Select route", all_routes,
                               index=all_routes.index(default_route))

route_yoy = run_query(f"""
    SELECT YEAR, MONTH,
           SUM(TOTAL_FLIGHTS)  AS TOTAL_FLIGHTS,
           SUM(DELAYED_FLIGHTS) AS DELAYED_FLIGHTS,
           ROUND(100.0 * SUM(DELAYED_FLIGHTS) / SUM(TOTAL_FLIGHTS), 2) AS DELAY_RATE,
           ROUND(AVG(AVG_DEP_DELAY_MIN), 2) AS AVG_DEP_DELAY_MIN
    FROM ANALYTICS.ROUTE_PERFORMANCE
    WHERE ROUTE = '{selected_route}'
      AND YEAR BETWEEN {year_start} AND {year_end}
    GROUP BY YEAR, MONTH
    ORDER BY YEAR, MONTH
""")

if not route_yoy.empty:
    yoy_annual = (
        route_yoy.groupby("YEAR")
        .agg(
            TOTAL_FLIGHTS=("TOTAL_FLIGHTS", "sum"),
            DELAY_RATE=("DELAY_RATE", "mean"),
            AVG_DEP_DELAY_MIN=("AVG_DEP_DELAY_MIN", "mean"),
        )
        .reset_index()
    )

    col_a, col_b = st.columns(2)
    with col_a:
        fig4a = px.line(
            yoy_annual, x="YEAR", y="DELAY_RATE",
            markers=True,
            title=f"{selected_route} — Annual Delay Rate",
            labels={"YEAR": "Year", "DELAY_RATE": "Delay Rate (%)"},
            color_discrete_sequence=["#EF553B"],
        )
        st.plotly_chart(fig4a, use_container_width=True)

    with col_b:
        fig4b = px.bar(
            yoy_annual, x="YEAR", y="TOTAL_FLIGHTS",
            title=f"{selected_route} — Annual Flights",
            labels={"YEAR": "Year", "TOTAL_FLIGHTS": "Total Flights"},
            color_discrete_sequence=["#636EFA"],
        )
        st.plotly_chart(fig4b, use_container_width=True)
else:
    st.info(f"No year-over-year data available for route {selected_route}.")

# ── Full route table ──────────────────────────────────────────────────────────
st.subheader("Full Route Table")

search = st.text_input("Search routes (e.g. JFK, LAX-ORD)")
display = filtered.copy()
if search:
    display = display[display["ROUTE"].str.contains(search.upper(), na=False)]

display_cols = ["ROUTE", "TOTAL_FLIGHTS", "DELAY_RATE", "AVG_DEP_DELAY_MIN", "AVG_DISTANCE"]
display = (
    display[display_cols]
    .rename(columns={
        "ROUTE": "Route",
        "TOTAL_FLIGHTS": "Total Flights",
        "DELAY_RATE": "Delay Rate (%)",
        "AVG_DEP_DELAY_MIN": "Avg Dep Delay (min)",
        "AVG_DISTANCE": "Avg Distance (mi)",
    })
    .sort_values("Delay Rate (%)", ascending=False)
    .reset_index(drop=True)
)
st.dataframe(
    display.style.format({
        "Total Flights": "{:,.0f}",
        "Delay Rate (%)": "{:.1f}",
        "Avg Dep Delay (min)": "{:.1f}",
        "Avg Distance (mi)": "{:.0f}",
    }).background_gradient(subset=["Delay Rate (%)"], cmap="RdYlGn_r"),
    use_container_width=True,
    height=400,
)
