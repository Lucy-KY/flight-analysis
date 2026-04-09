"""
Page 7 — Flight Delay Predictor
================================
XGBoost-powered delay prediction for a single upcoming flight.
Models trained on 25 years of BTS historical data via train_delay_model.py.
"""

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ── Path setup — allow imports from code/ root ────────────────────────────────
_CODE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_CODE_DIR))

from models.predict import load_models, models_available, predict_flight
from utils.db import AIRLINE_NAMES, run_query
from config.airport_coords import AIRPORT_COORDS

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Flight Delay Predictor",
    page_icon="🔮",
    layout="wide",
)

st.title("🔮 Flight Delay Predictor")
st.markdown(
    "Predict the on-time probability and expected delay for an upcoming flight. "
    "The model is an XGBoost ensemble trained on **25 years of BTS historical data** "
    "(2000–2024) with weather integration."
)

# ── Guard: models must be trained ─────────────────────────────────────────────
if not models_available():
    st.warning(
        "Models not trained yet. Run:\n\n"
        "```bash\n"
        "cd code/\n"
        "python models/train_delay_model.py --sample-frac 0.1\n"
        "```"
    )
    st.stop()

# ── Load model metadata for the info expander ─────────────────────────────────
_, _, _, metrics = load_models()

# ── Airport name lookup (top 50 from AIRPORT_COORDS) ─────────────────────────
_AIRPORT_NAMES: dict[str, str] = {
    "ATL": "Hartsfield-Jackson Atlanta",
    "DFW": "Dallas/Fort Worth",
    "DEN": "Denver International",
    "ORD": "O'Hare International",
    "LAX": "Los Angeles International",
    "CLT": "Charlotte Douglas",
    "LAS": "Harry Reid International",
    "PHX": "Phoenix Sky Harbor",
    "MCO": "Orlando International",
    "SEA": "Seattle-Tacoma",
    "MIA": "Miami International",
    "IAH": "George Bush Intercontinental",
    "JFK": "John F. Kennedy International",
    "EWR": "Newark Liberty",
    "MSP": "Minneapolis-Saint Paul",
    "BOS": "Boston Logan",
    "DTW": "Detroit Metropolitan",
    "PHL": "Philadelphia International",
    "LGA": "LaGuardia",
    "FLL": "Fort Lauderdale-Hollywood",
    "BWI": "Baltimore/Washington",
    "DCA": "Ronald Reagan Washington National",
    "MDW": "Chicago Midway",
    "SFO": "San Francisco International",
    "SLC": "Salt Lake City International",
    "IAD": "Dulles International",
    "SAN": "San Diego International",
    "TPA": "Tampa International",
    "PDX": "Portland International",
    "HOU": "William P. Hobby",
    "STL": "St. Louis Lambert",
    "BNA": "Nashville International",
    "OAK": "Oakland International",
    "MCI": "Kansas City International",
    "SMF": "Sacramento International",
    "RDU": "Raleigh-Durham",
    "SJC": "San Jose Mineta",
    "MSY": "Louis Armstrong New Orleans",
    "SAT": "San Antonio International",
    "CLE": "Cleveland Hopkins",
    "CVG": "Cincinnati/Northern Kentucky",
    "PIT": "Pittsburgh International",
    "CMH": "John Glenn Columbus",
    "IND": "Indianapolis International",
    "MKE": "Milwaukee Mitchell",
    "AUS": "Austin-Bergstrom",
    "OGG": "Kahului (Maui)",
    "HNL": "Daniel K. Inouye (Honolulu)",
    "ANC": "Ted Stevens Anchorage",
    "SNA": "John Wayne (Orange County)",
}


def _airport_label(code: str) -> str:
    name = _AIRPORT_NAMES.get(code, "")
    return f"{code} — {name}" if name else code


def _airline_label(code: str) -> str:
    name = AIRLINE_NAMES.get(code, "")
    return f"{name} ({code})" if name else code


# Sorted lists for selectboxes
_AIRPORT_CODES = sorted(AIRPORT_COORDS.keys())
_AIRPORT_OPTIONS = [_airport_label(c) for c in _AIRPORT_CODES]
_AIRLINE_CODES = sorted(AIRLINE_NAMES.keys())
_AIRLINE_OPTIONS = [_airline_label(c) for c in _AIRLINE_CODES]


def _code_from_label(label: str, codes: list[str]) -> str:
    """Extract the raw code from a formatted 'CODE — Name' label."""
    return label.split(" — ")[0].split(" (")[0].strip()


# ── Input form ────────────────────────────────────────────────────────────────
with st.form("prediction_form"):
    st.subheader("Flight Details")

    # Row 1: origin / destination / airline
    col1, col2, col3 = st.columns(3)
    with col1:
        origin_label = st.selectbox(
            "Origin Airport",
            options=_AIRPORT_OPTIONS,
            index=_AIRPORT_CODES.index("ATL") if "ATL" in _AIRPORT_CODES else 0,
        )
    with col2:
        dest_label = st.selectbox(
            "Destination Airport",
            options=_AIRPORT_OPTIONS,
            index=_AIRPORT_CODES.index("LAX") if "LAX" in _AIRPORT_CODES else 1,
        )
    with col3:
        airline_label_sel = st.selectbox(
            "Airline",
            options=_AIRLINE_OPTIONS,
            index=_AIRLINE_CODES.index("AA") if "AA" in _AIRLINE_CODES else 0,
        )

    # Row 2: date / hour / distance
    col4, col5, col6 = st.columns(3)
    with col4:
        dep_date = st.date_input("Departure Date", value=date.today())
    with col5:
        dep_hour = st.slider("Departure Hour (local)", min_value=0, max_value=23, value=8)
    with col6:
        distance = st.number_input(
            "Distance (miles)", min_value=0.0, max_value=6000.0, value=500.0, step=10.0
        )

    # Row 3: scheduled elapsed time
    col7, _, _ = st.columns(3)
    with col7:
        sched_elapsed = st.number_input(
            "Scheduled Elapsed Time (min)", min_value=0.0, max_value=1000.0,
            value=90.0, step=5.0,
        )

    # Origin weather
    with st.expander("Origin Weather (optional)"):
        ow_col1, ow_col2, ow_col3, ow_col4 = st.columns(4)
        with ow_col1:
            o_temp     = st.number_input("Temp (°C)",     min_value=-20.0, max_value=45.0,  value=15.0, key="o_temp")
            o_precip   = st.number_input("Precip (mm)",   min_value=0.0,   max_value=100.0, value=0.0,  key="o_precip")
        with ow_col2:
            o_snow     = st.number_input("Snowfall (cm)", min_value=0.0,   max_value=50.0,  value=0.0,  key="o_snow")
            o_wind     = st.number_input("Wind (km/h)",   min_value=0.0,   max_value=150.0, value=10.0, key="o_wind")
        with ow_col3:
            o_gust     = st.number_input("Gust (km/h)",   min_value=0.0,   max_value=200.0, value=12.0, key="o_gust")
            o_vis      = st.number_input("Visibility (m)", min_value=0.0, max_value=50000.0, value=10000.0, key="o_vis")
        with ow_col4:
            o_cloud    = st.number_input("Cloud Cover (%)", min_value=0.0, max_value=100.0, value=50.0, key="o_cloud")
            o_wx_code  = st.number_input("WX Code",         min_value=0,   max_value=99,    value=0,    step=1, key="o_wx")

    # Destination weather
    with st.expander("Destination Weather (optional)"):
        dw_col1, dw_col2, dw_col3, dw_col4 = st.columns(4)
        with dw_col1:
            d_temp     = st.number_input("Temp (°C)",     min_value=-20.0, max_value=45.0,  value=15.0, key="d_temp")
            d_precip   = st.number_input("Precip (mm)",   min_value=0.0,   max_value=100.0, value=0.0,  key="d_precip")
        with dw_col2:
            d_snow     = st.number_input("Snowfall (cm)", min_value=0.0,   max_value=50.0,  value=0.0,  key="d_snow")
            d_wind     = st.number_input("Wind (km/h)",   min_value=0.0,   max_value=150.0, value=10.0, key="d_wind")
        with dw_col3:
            d_gust     = st.number_input("Gust (km/h)",   min_value=0.0,   max_value=200.0, value=12.0, key="d_gust")
            d_vis      = st.number_input("Visibility (m)", min_value=0.0, max_value=50000.0, value=10000.0, key="d_vis")
        with dw_col4:
            d_cloud    = st.number_input("Cloud Cover (%)", min_value=0.0, max_value=100.0, value=50.0, key="d_cloud")
            d_wx_code  = st.number_input("WX Code",         min_value=0,   max_value=99,    value=0,    step=1, key="d_wx")

    submitted = st.form_submit_button("Predict Delay", use_container_width=True)

# ── Results ───────────────────────────────────────────────────────────────────
if submitted:
    origin  = _code_from_label(origin_label, _AIRPORT_CODES)
    dest    = _code_from_label(dest_label, _AIRPORT_CODES)
    carrier = _code_from_label(airline_label_sel, _AIRLINE_CODES)

    # Derive DOW and month from selected date (Snowflake: DAYOFWEEK 1=Sun … 7=Sat)
    dep_dow   = dep_date.isoweekday() % 7 + 1   # Python Mon=1 → Sun=1..Sat=7
    dep_month = dep_date.month

    origin_weather = {
        "temp_c": o_temp, "precip_mm": o_precip, "snowfall_cm": o_snow,
        "wind_kmh": o_wind, "gust_kmh": o_gust, "visibility_m": o_vis,
        "cloud_pct": o_cloud, "wx_code": int(o_wx_code),
    }
    dest_weather = {
        "temp_c": d_temp, "precip_mm": d_precip, "snowfall_cm": d_snow,
        "wind_kmh": d_wind, "gust_kmh": d_gust, "visibility_m": d_vis,
        "cloud_pct": d_cloud, "wx_code": int(d_wx_code),
    }

    result = predict_flight(
        origin=origin, dest=dest, carrier=carrier,
        dep_hour=dep_hour, dep_dow=dep_dow, dep_month=dep_month,
        distance=float(distance), sched_elapsed_min=float(sched_elapsed),
        origin_weather=origin_weather, dest_weather=dest_weather,
    )

    if "error" in result:
        st.error(f"Prediction failed: {result['error']}")
        st.stop()

    on_time_pct   = result["on_time_probability"] * 100
    delay_pct     = result["delay_probability"] * 100
    exp_delay_min = result["expected_delay_min"]
    risk_level    = result["risk_level"]
    risk_color    = result["risk_color"]

    st.divider()
    st.subheader("Prediction Results")

    # Three metric cards
    m1, m2, m3 = st.columns(3)
    m1.metric("On-Time Probability", f"{on_time_pct:.1f}%")
    m2.metric("Expected Delay",      f"+{exp_delay_min:.0f} min")
    m3.metric("Risk Level",          risk_level)

    # Plotly gauge chart
    _gauge_colors = {
        "green":   "#2ecc71",
        "orange":  "#f39c12",
        "red":     "#e74c3c",
        "darkred": "#922b21",
    }
    bar_color = _gauge_colors.get(risk_color, "#3498db")

    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=on_time_pct,
        number={"suffix": "%", "font": {"size": 36}},
        delta={"reference": 80, "suffix": "%", "relative": False},
        title={"text": "On-Time Probability", "font": {"size": 18}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1},
            "bar":  {"color": bar_color},
            "steps": [
                {"range": [0,  40], "color": "#fadbd8"},
                {"range": [40, 60], "color": "#fdebd0"},
                {"range": [60, 80], "color": "#fef9e7"},
                {"range": [80, 100], "color": "#eafaf1"},
            ],
            "threshold": {
                "line": {"color": "black", "width": 3},
                "thickness": 0.75,
                "value": 80,
            },
        },
    ))
    fig_gauge.update_layout(height=320, margin={"t": 60, "b": 20, "l": 20, "r": 20})
    st.plotly_chart(fig_gauge, use_container_width=True)

    # Historical context from ANALYTICS
    st.subheader("Historical Route Context")
    try:
        hist_df = run_query(f"""
            SELECT ORIGIN, DEST, AVG_DEP_DELAY_MIN, TOTAL_FLIGHTS
            FROM ANALYTICS.ROUTE_PERFORMANCE
            WHERE ORIGIN = '{origin}' AND DEST = '{dest}'
            LIMIT 1
        """)
        if not hist_df.empty:
            avg_hist = float(hist_df["AVG_DEP_DELAY_MIN"].iloc[0])
            tot_hist = int(hist_df["TOTAL_FLIGHTS"].iloc[0])
            st.info(
                f"Historical avg departure delay for **{origin} → {dest}**: "
                f"**{avg_hist:.1f} min** (based on {tot_hist:,} historical flights)"
            )
        else:
            st.info(f"No historical route data found for {origin} → {dest}.")
    except Exception as exc:
        st.info(f"Could not fetch historical route data: {exc}")

# ── Model info expander ───────────────────────────────────────────────────────
with st.expander("Model Information"):
    clf_m = metrics.get("classifier", {})
    reg_m = metrics.get("regressor", {})

    info_cols = st.columns(2)
    with info_cols[0]:
        st.markdown("**Classifier (XGBClassifier)**")
        st.write(f"- Accuracy:  {clf_m.get('accuracy', 'N/A'):.4f}" if isinstance(clf_m.get('accuracy'), float) else "- Accuracy: N/A")
        st.write(f"- F1 Score:  {clf_m.get('f1', 'N/A'):.4f}" if isinstance(clf_m.get('f1'), float) else "- F1 Score: N/A")
        st.write(f"- AUC-ROC:   {clf_m.get('auc_roc', 'N/A'):.4f}" if isinstance(clf_m.get('auc_roc'), float) else "- AUC-ROC: N/A")

    with info_cols[1]:
        st.markdown("**Regressor (XGBRegressor)**")
        st.write(f"- RMSE: {reg_m.get('rmse', 'N/A'):.2f} min" if isinstance(reg_m.get('rmse'), float) else "- RMSE: N/A")
        st.write(f"- MAE:  {reg_m.get('mae', 'N/A'):.2f} min" if isinstance(reg_m.get('mae'), float) else "- MAE: N/A")

    st.markdown("---")
    st.write(f"**Training rows:** {metrics.get('training_rows', 'N/A'):,}" if isinstance(metrics.get('training_rows'), int) else f"**Training rows:** {metrics.get('training_rows', 'N/A')}")
    st.write(f"**Sample fraction:** {metrics.get('sample_frac', 'N/A')}")
    st.write(f"**Trained at:** {metrics.get('trained_at', 'N/A')}")
    st.write(f"**Weather features:** {metrics.get('use_weather', 'N/A')}")
    st.write(f"**Features ({len(metrics.get('feature_names', []))}):** {', '.join(metrics.get('feature_names', []))}")
