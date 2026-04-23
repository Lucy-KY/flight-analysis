"""
Page 1 — Delay Trends
======================
Monthly and annual delay trend charts with year-range filtering.
"""

import sys
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.db import run_query

st.set_page_config(page_title="Delay Trends", page_icon="📈", layout="wide")
st.title("📈 Delay Trends")
st.markdown("Explore how U.S. flight delay rates have changed over time, by year and by season.")

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.header("Filters")

try:
    year_range_df = run_query(
        "SELECT MIN(YEAR) AS MIN_Y, MAX(YEAR) AS MAX_Y FROM ANALYTICS.DELAY_TRENDS_MONTHLY"
    )
    min_year = int(year_range_df["MIN_Y"].iloc[0])
    max_year = int(year_range_df["MAX_Y"].iloc[0])
except Exception:
    min_year, max_year = 2000, 2024

year_start, year_end = st.sidebar.slider(
    "Year range", min_year, max_year, (min_year, max_year), step=1
)

# ── Load data ─────────────────────────────────────────────────────────────────
monthly_df = run_query(f"""
    SELECT YEAR, MONTH,
           TOTAL_FLIGHTS, TOTAL_DELAYED,
           DELAY_RATE, AVG_DEP_DELAY_MIN, AVG_ARR_DELAY_MIN,
           CARRIER_DELAY_SHARE, WEATHER_DELAY_SHARE, NAS_DELAY_SHARE
    FROM ANALYTICS.DELAY_TRENDS_MONTHLY
    WHERE YEAR BETWEEN {year_start} AND {year_end}
    ORDER BY YEAR, MONTH
""")

if monthly_df.empty:
    st.warning("No data available for the selected year range.")
    st.stop()

# Derived columns
monthly_df["PERIOD"] = (
    monthly_df["YEAR"].astype(str) + "-"
    + monthly_df["MONTH"].astype(str).str.zfill(2)
)

# Yearly aggregate
yearly_df = (
    monthly_df.groupby("YEAR")
    .agg(
        TOTAL_FLIGHTS=("TOTAL_FLIGHTS", "sum"),
        TOTAL_DELAYED=("TOTAL_DELAYED", "sum"),
        AVG_DEP_DELAY_MIN=("AVG_DEP_DELAY_MIN", "mean"),
    )
    .reset_index()
)
yearly_df["DELAY_RATE"] = (
    100.0 * yearly_df["TOTAL_DELAYED"] / yearly_df["TOTAL_FLIGHTS"]
).round(2)

# ── Chart 1: Annual delay & cancellation rate ─────────────────────────────────
st.subheader(f"Annual Delay Rate ({year_start}–{year_end})")

fig1 = go.Figure()
fig1.add_trace(go.Scatter(
    x=yearly_df["YEAR"], y=yearly_df["DELAY_RATE"],
    name="Delay Rate (%)",
    mode="lines+markers",
    line=dict(color="#EF553B", width=2),
    marker=dict(size=6),
))
fig1.update_layout(
    xaxis_title="Year",
    yaxis_title="Delay Rate (%)",
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)
st.plotly_chart(fig1, use_container_width=True)

# ── Chart 2: Monthly trend line (one line per year if ≤ 5 years) ──────────────
st.subheader("Monthly Delay Rate by Year")

n_years = year_end - year_start + 1
if n_years <= 6:
    # Show individual year lines
    month_labels = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                    7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
    monthly_df["MONTH_LABEL"] = monthly_df["MONTH"].map(month_labels)
    fig2 = px.line(
        monthly_df,
        x="MONTH", y="DELAY_RATE",
        color="YEAR",
        markers=True,
        title="Monthly Delay Rate — Each Year",
        labels={"MONTH": "Month", "DELAY_RATE": "Delay Rate (%)", "YEAR": "Year"},
        color_discrete_sequence=px.colors.qualitative.Plotly,
    )
    fig2.update_xaxes(tickvals=list(range(1, 13)),
                      ticktext=list(month_labels.values()))
    st.plotly_chart(fig2, use_container_width=True)
else:
    # Aggregate across years to show seasonal pattern only
    seasonal = (
        monthly_df.groupby("MONTH")
        .agg(AVG_DELAY_RATE=("DELAY_RATE", "mean"),
             AVG_DEP_DELAY_MIN=("AVG_DEP_DELAY_MIN", "mean"))
        .reset_index()
    )
    month_labels = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                    7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
    seasonal["MONTH_LABEL"] = seasonal["MONTH"].map(month_labels)
    fig2 = px.bar(
        seasonal, x="MONTH_LABEL", y="AVG_DELAY_RATE",
        title=f"Average Monthly Delay Rate ({year_start}–{year_end})",
        labels={"MONTH_LABEL": "Month", "AVG_DELAY_RATE": "Avg Delay Rate (%)"},
        color="AVG_DELAY_RATE",
        color_continuous_scale="RdYlGn_r",
        text_auto=".1f",
    )
    fig2.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig2, use_container_width=True)

# ── Chart 3: Average departure delay minutes over time ────────────────────────
st.subheader("Average Departure Delay (minutes) — Annual")

fig3 = px.area(
    yearly_df, x="YEAR", y="AVG_DEP_DELAY_MIN",
    title="Average Departure Delay per Delayed Flight (minutes)",
    labels={"YEAR": "Year", "AVG_DEP_DELAY_MIN": "Avg Dep Delay (min)"},
    color_discrete_sequence=["#636EFA"],
)
fig3.update_layout(hovermode="x unified")
st.plotly_chart(fig3, use_container_width=True)

# ── Delay cause share over time ───────────────────────────────────────────────
st.subheader("Delay Cause Share Over Time")

cause_yearly = (
    monthly_df.groupby("YEAR")
    .agg(
        CARRIER=("CARRIER_DELAY_SHARE", "mean"),
        WEATHER=("WEATHER_DELAY_SHARE", "mean"),
        NAS=("NAS_DELAY_SHARE", "mean"),
    )
    .reset_index()
)

fig4 = go.Figure()
for col, color, label in [
    ("CARRIER", "#EF553B", "Carrier"),
    ("WEATHER", "#636EFA", "Weather"),
    ("NAS",     "#00CC96", "NAS"),
]:
    fig4.add_trace(go.Scatter(
        x=cause_yearly["YEAR"], y=cause_yearly[col],
        name=label, stackgroup="one",
        line=dict(color=color),
        hoverinfo="x+y+name",
    ))
fig4.update_layout(
    title="Delay Cause Share (% of Total Delay Minutes)",
    xaxis_title="Year",
    yaxis_title="Share (%)",
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02),
)
st.plotly_chart(fig4, use_container_width=True)

# ── Raw data expander ─────────────────────────────────────────────────────────
with st.expander("View raw monthly data"):
    st.dataframe(monthly_df.drop(columns=["PERIOD", "MONTH_LABEL"], errors="ignore"),
                 use_container_width=True)
