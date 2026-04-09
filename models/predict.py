"""
predict.py — Inference module for flight delay prediction
===========================================================
Load trained XGBoost models and generate delay predictions for a single flight.
Models loaded from the same directory as this file (code/models/).

Imported by visualization/pages/7_Delay_Predictor.py
"""

import json
import numpy as np
import pandas as pd
import joblib
from functools import lru_cache
from pathlib import Path

MODELS_DIR = Path(__file__).parent

# ── Default weather values used when keys are missing ────────────────────────
_WX_DEFAULTS = {
    "temp_c":       15.0,
    "precip_mm":     0.0,
    "snowfall_cm":   0.0,
    "wind_kmh":     10.0,
    "gust_kmh":     12.0,
    "visibility_m": 10000.0,
    "cloud_pct":    50.0,
    "wx_code":       0,
}


def models_available() -> bool:
    """Return True if all required model files exist."""
    required = [
        "delay_classifier.joblib",
        "delay_regressor.joblib",
        "feature_names.json",
        "model_metrics.json",
    ]
    return all((MODELS_DIR / f).exists() for f in required)


@lru_cache(maxsize=1)
def load_models():
    """
    Load classifier and regressor pipelines plus metadata.
    Results are cached after the first call (lru_cache).

    Returns:
        clf          : trained sklearn Pipeline (preprocessor + XGBClassifier)
        reg          : trained sklearn Pipeline (preprocessor + XGBRegressor)
        feature_names: ordered list of feature column names
        metrics      : dict from model_metrics.json
    """
    clf = joblib.load(MODELS_DIR / "delay_classifier.joblib")
    reg = joblib.load(MODELS_DIR / "delay_regressor.joblib")

    with open(MODELS_DIR / "feature_names.json") as f:
        feature_names = json.load(f)

    with open(MODELS_DIR / "model_metrics.json") as f:
        metrics = json.load(f)

    return clf, reg, feature_names, metrics


def _build_row(
    origin: str,
    dest: str,
    carrier: str,
    dep_hour: int,
    dep_dow: int,
    dep_month: int,
    distance: float,
    sched_elapsed_min: float,
    origin_weather: dict,
    dest_weather: dict,
    feature_names: list[str],
) -> pd.DataFrame:
    """Build a single-row DataFrame matching the training feature schema."""
    ow = {**_WX_DEFAULTS, **(origin_weather or {})}
    dw = {**_WX_DEFAULTS, **(dest_weather or {})}

    row = {
        "DEP_HOUR":          int(dep_hour),
        "DEP_DOW":           int(dep_dow),
        "DEP_MONTH":         int(dep_month),
        "FLIGHT_DISTANCE":   float(distance),
        "SCHED_ELAPSED_MIN": float(sched_elapsed_min),
        # Weather — origin
        "ORIGIN_TEMP_C":      float(ow["temp_c"]),
        "ORIGIN_PRECIP_MM":   float(ow["precip_mm"]),
        "ORIGIN_SNOWFALL_CM": float(ow["snowfall_cm"]),
        "ORIGIN_WIND_KMH":    float(ow["wind_kmh"]),
        "ORIGIN_GUST_KMH":    float(ow["gust_kmh"]),
        "ORIGIN_VIS_M":       float(ow["visibility_m"]),
        "ORIGIN_CLOUD_PCT":   float(ow["cloud_pct"]),
        "ORIGIN_WX_CODE":     int(ow["wx_code"]),
        # Weather — destination
        "DEST_TEMP_C":    float(dw["temp_c"]),
        "DEST_WIND_KMH":  float(dw["wind_kmh"]),
        "DEST_VIS_M":     float(dw["visibility_m"]),
        "DEST_WX_CODE":   int(dw["wx_code"]),
        # Categoricals
        "OP_CARRIER": str(carrier),
        "ORIGIN":     str(origin),
        "DEST":       str(dest),
    }

    # Only keep columns the model was trained on
    filtered = {k: row[k] for k in feature_names if k in row}
    # For any feature_name not in our row dict, fill with 0
    for k in feature_names:
        if k not in filtered:
            filtered[k] = 0

    return pd.DataFrame([filtered])[feature_names]


def _risk_label(on_time_prob: float) -> tuple[str, str]:
    """Return (risk_level, risk_color) based on on-time probability."""
    if on_time_prob >= 0.80:
        return "Low", "green"
    elif on_time_prob >= 0.60:
        return "Medium", "orange"
    elif on_time_prob >= 0.40:
        return "High", "red"
    else:
        return "Severe", "darkred"


def predict_flight(
    origin: str,
    dest: str,
    carrier: str,
    dep_hour: int,
    dep_dow: int,
    dep_month: int,
    distance: float,
    sched_elapsed_min: float,
    origin_weather: dict,
    dest_weather: dict,
) -> dict:
    """
    Generate delay predictions for a single flight.

    Parameters
    ----------
    origin / dest / carrier : IATA / carrier codes (strings)
    dep_hour   : departure hour 0–23
    dep_dow    : day of week 1–7 (Snowflake convention)
    dep_month  : month 1–12
    distance   : flight distance in miles
    sched_elapsed_min : scheduled flight duration in minutes
    origin_weather / dest_weather : dicts with optional keys:
        temp_c, precip_mm, snowfall_cm, wind_kmh, gust_kmh,
        visibility_m, cloud_pct, wx_code

    Returns
    -------
    dict with keys:
        on_time_probability  (float, 0–1)
        delay_probability    (float, 0–1)
        predicted_delay_min  (float, >=0)   — regressor output if delayed
        expected_delay_min   (float, >=0)   — delay_probability * predicted_delay_min
        risk_level           (str)          — "Low" / "Medium" / "High" / "Severe"
        risk_color           (str)          — "green" / "orange" / "red" / "darkred"
    or:
        {"error": "Models not trained yet"}  if model files are missing
    """
    if not models_available():
        return {"error": "Models not trained yet"}

    try:
        clf, reg, feature_names, _ = load_models()

        X = _build_row(
            origin=origin,
            dest=dest,
            carrier=carrier,
            dep_hour=dep_hour,
            dep_dow=dep_dow,
            dep_month=dep_month,
            distance=distance,
            sched_elapsed_min=sched_elapsed_min,
            origin_weather=origin_weather,
            dest_weather=dest_weather,
            feature_names=feature_names,
        )

        delay_prob     = float(clf.predict_proba(X)[0, 1])
        on_time_prob   = 1.0 - delay_prob
        pred_delay_min = float(np.clip(reg.predict(X)[0], 0, 300))
        expected_delay = delay_prob * pred_delay_min

        risk_level, risk_color = _risk_label(on_time_prob)

        return {
            "on_time_probability":  round(on_time_prob, 4),
            "delay_probability":    round(delay_prob, 4),
            "predicted_delay_min":  round(pred_delay_min, 1),
            "expected_delay_min":   round(expected_delay, 1),
            "risk_level":           risk_level,
            "risk_color":           risk_color,
        }

    except Exception as exc:
        return {"error": str(exc)}
