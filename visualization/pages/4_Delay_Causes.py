"""
Page 4 — Delay Cause Breakdown
================================
Visualize how Carrier, Weather, NAS, Security, and Late Aircraft
delays contribute to overall delay minutes over time.
"""

import sys
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.db import run_query

st.set_page_config(page_title="Delay Causes", page_icon="🔍", layout="wide")
st.title("🔍 Delay Cause Breakdown")
st.markdown(
    "Understand what drives U.S. flight delays — carrier operations, weather, "
    "NAS congestion, security, or late arriving aircraft."
)

CAUSE_LABELS = {
    "CARRIER_DELAY_TOTAL_MIN":       "Carrier",
    "WEATHER_DELAY_TOTAL_MIN":       "Weather",
    "NAS_DELAY_TOTAL_MIN":           "NAS",
    "SECURITY_DELAY_TOTAL_MIN":      "Security",
    "LATE_AIRCRAFT_DELAY_TOTAL_MIN": "Late Aircraft",
}

CAUSE_COLORS = {
    "Carrier":       "#EF553B",
    "Weather":       "#636EFA",
    "NAS":           "#00CC96",
    "Security":      "#AB63FA",
    "Late Aircraft": "#FFA15A",
}

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.header("Filters")

try:
    yr_df = run_query(
        "SELECT MIN(YEAR) AS MIN_Y, MAX(YEAR) AS MAX_Y FROM ANALYTICS.DELAY_CAUSE_BREAKDOWN"
    )
    min_year = int(yr_df["MIN_Y"].iloc[0])
    max_year = int(yr_df["MAX_Y"].iloc[0])
except Exception:
    min_year, max_year = 2000, 2024

year_start, year_end = st.sidebar.slider(
    "Year range", min_year, max_year, (min_year, max_year)
)

# ── Load data ─────────────────────────────────────────────────────────────────
raw_df = run_query(f"""
    SELECT YEAR, MONTH,
           CARRIER_DELAY_FLIGHTS, WEATHER_DELAY_FLIGHTS, NAS_DELAY_FLIGHTS,
           SECURITY_DELAY_FLIGHTS, LATE_AIRCRAFT_DELAY_FLIGHTS,
           CARRIER_DELAY_TOTAL_MIN, WEATHER_DELAY_TOTAL_MIN, NAS_DELAY_TOTAL_MIN,
           SECURITY_DELAY_TOTAL_MIN, LATE_AIRCRAFT_DELAY_TOTAL_MIN
    FROM ANALYTICS.DELAY_CAUSE_BREAKDOWN
    WHERE YEAR BETWEEN {year_start} AND {year_end}
    ORDER BY YEAR, MONTH
""")

if raw_df.empty:
    st.warning("No delay cause data for the selected period.")
    st.stop()

# ── Aggregate totals ──────────────────────────────────────────────────────────
totals = {
    label: raw_df[col].sum()
    for col, label in CAUSE_LABELS.items()
}
grand_total = sum(totals.values())

# ── Chart 1: Pie chart of delay cause shares ──────────────────────────────────
st.subheader(f"Overall Delay Cause Share ({year_start}–{year_end})")

col1, col2 = st.columns([1, 1])
with col1:
    pie_df = {
        "Cause": list(totals.keys()),
        "Total Minutes": list(totals.values()),
    }
    import pandas as pd
    pie_df = pd.DataFrame(pie_df)
    fig1 = px.pie(
        pie_df, names="Cause", values="Total Minutes",
        color="Cause",
        color_discrete_map=CAUSE_COLORS,
        hole=0.4,
        title="Share of Total Delay Minutes",
    )
    fig1.update_traces(textposition="outside", textinfo="percent+label")
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.markdown("#### Delay Cause Summary")
    st.markdown(f"**Total delay minutes:** {grand_total:,.0f}")
    for cause, mins in sorted(totals.items(), key=lambda x: -x[1]):
        pct = 100 * mins / grand_total if grand_total else 0
        st.markdown(
            f"- **{cause}:** {mins:,.0f} min &nbsp;&nbsp; "
            f"<span style='color:gray'>({pct:.1f}%)</span>",
            unsafe_allow_html=True,
        )
    st.markdown("""
---
**Legend:**
- **Carrier** — Airline-caused (mechanical, crew, etc.)
- **Weather** — Weather at origin/destination
- **NAS** — National Airspace System (ATC, congestion)
- **Security** — Security screening delays
- **Late Aircraft** — Delay from previous leg
""")

# ── Chart 2: Stacked area chart by year ──────────────────────────────────────
st.subheader("Delay Cause Trends Over Years")

yearly = (
    raw_df.groupby("YEAR")[list(CAUSE_LABELS.keys())]
    .sum()
    .reset_index()
)

fig2 = go.Figure()
for col, label in CAUSE_LABELS.items():
    fig2.add_trace(go.Scatter(
        x=yearly["YEAR"],
        y=yearly[col],
        name=label,
        stackgroup="one",
        line=dict(color=CAUSE_COLORS[label]),
        hoverinfo="x+y+name",
    ))
fig2.update_layout(
    title="Total Delay Minutes by Cause (Stacked)",
    xaxis_title="Year",
    yaxis_title="Total Delay Minutes",
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02),
)
st.plotly_chart(fig2, use_container_width=True)

# ── Chart 3: Cause percentage share by year (normalized) ─────────────────────
st.subheader("Delay Cause Share (%) by Year — Normalized")

for col, label in CAUSE_LABELS.items():
    yearly[f"{label}_PCT"] = 100.0 * yearly[col] / yearly[list(CAUSE_LABELS.keys())].sum(axis=1)

fig3 = go.Figure()
for col, label in CAUSE_LABELS.items():
    fig3.add_trace(go.Bar(
        x=yearly["YEAR"],
        y=yearly[f"{label}_PCT"].round(1),
        name=label,
        marker_color=CAUSE_COLORS[label],
    ))
fig3.update_layout(
    barmode="stack",
    title="Delay Cause Share (%) by Year",
    xaxis_title="Year",
    yaxis_title="Share (%)",
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02),
)
st.plotly_chart(fig3, use_container_width=True)

# ── Chart 4: Seasonal pattern by month ───────────────────────────────────────
st.subheader("Seasonal Delay Cause Pattern")

monthly_avg = (
    raw_df.groupby("MONTH")[list(CAUSE_LABELS.keys())]
    .mean()
    .reset_index()
)
month_labels = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
monthly_avg["MONTH_LABEL"] = monthly_avg["MONTH"].map(month_labels)

fig4 = go.Figure()
for col, label in CAUSE_LABELS.items():
    fig4.add_trace(go.Bar(
        x=monthly_avg["MONTH_LABEL"],
        y=monthly_avg[col].round(0),
        name=label,
        marker_color=CAUSE_COLORS[label],
    ))
fig4.update_layout(
    barmode="stack",
    title="Average Delay Minutes by Cause and Month",
    xaxis_title="Month",
    yaxis_title="Avg Delay Minutes",
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02),
)
st.plotly_chart(fig4, use_container_width=True)

# ── Insight callouts ──────────────────────────────────────────────────────────
with st.expander("💡 Key Insights"):
    top_cause = max(totals, key=totals.get)
    st.markdown(f"""
- The **#{1} delay cause** by total minutes is **{top_cause}**, accounting for
  **{100 * totals[top_cause] / grand_total:.1f}%** of all delay time
  from {year_start} to {year_end}.
- **Weather** delays tend to spike in **December–February** (winter storms)
  and **June–August** (thunderstorm season).
- **NAS (National Airspace System)** delays reflect ATC capacity constraints
  and tend to be highest at busy hub airports.
- **Late Aircraft** delays cascade through the day — an early-morning delay
  propagates to every subsequent leg operated by that aircraft.
""")

# ── Raw data ──────────────────────────────────────────────────────────────────
with st.expander("View raw monthly breakdown data"):
    st.dataframe(raw_df, use_container_width=True)
