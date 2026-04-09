"""
train_delay_model.py — XGBoost flight delay classifier + regressor
====================================================================
Trains two models on 25 years of BTS historical data from Snowflake:
  - delay_classifier.joblib  : XGBClassifier — predict IS_DEP_DELAYED (bool)
  - delay_regressor.joblib   : XGBRegressor  — predict DEP_DELAY_MIN (minutes)

Usage:
    cd code/
    python models/train_delay_model.py                      # full dataset
    python models/train_delay_model.py --sample-frac 0.1    # 10% sample (dev)
    python models/train_delay_model.py --no-weather          # skip weather features
    python models/train_delay_model.py --output-dir models/  # custom output dir
"""

import argparse
import json
import os
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sklearn.compose import ColumnTransformer
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import TargetEncoder
import joblib
import xgboost as xgb

warnings.filterwarnings("ignore")

# ── Project root & env ────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")
sys.path.insert(0, str(BASE_DIR))

from config.snowflake_conn import get_conn


# ── Constants ─────────────────────────────────────────────────────────────────
NUMERIC_FEATURES = [
    "DEP_HOUR", "DEP_DOW", "DEP_MONTH", "FLIGHT_DISTANCE",
    "SCHED_ELAPSED_MIN",
]

WEATHER_FEATURES = [
    "ORIGIN_TEMP_C", "ORIGIN_PRECIP_MM", "ORIGIN_SNOWFALL_CM",
    "ORIGIN_WIND_KMH", "ORIGIN_GUST_KMH", "ORIGIN_VIS_M",
    "ORIGIN_CLOUD_PCT", "ORIGIN_WX_CODE",
    "DEST_TEMP_C", "DEST_WIND_KMH", "DEST_VIS_M", "DEST_WX_CODE",
]

CATEGORICAL_FEATURES = ["OP_CARRIER", "ORIGIN", "DEST"]

LABEL_CLASS = "LABEL_CLASS"
LABEL_REG   = "LABEL_REG"


def build_query(sample_frac: float | None, use_weather: bool) -> str:
    sample_clause = ""
    if sample_frac and sample_frac < 1.0:
        sample_clause = f"TABLESAMPLE ({sample_frac * 100:.1f})"

    if use_weather:
        weather_cols = """
    COALESCE(ORIGIN_TEMP_C, 15.0)::FLOAT     AS ORIGIN_TEMP_C,
    COALESCE(ORIGIN_PRECIP_MM, 0.0)::FLOAT   AS ORIGIN_PRECIP_MM,
    COALESCE(ORIGIN_SNOWFALL_CM, 0.0)::FLOAT AS ORIGIN_SNOWFALL_CM,
    COALESCE(ORIGIN_WIND_SPEED_KMH, 10.0)::FLOAT AS ORIGIN_WIND_KMH,
    COALESCE(ORIGIN_WIND_GUST_KMH, 12.0)::FLOAT  AS ORIGIN_GUST_KMH,
    COALESCE(ORIGIN_VISIBILITY_M, 10000.0)::FLOAT AS ORIGIN_VIS_M,
    COALESCE(ORIGIN_CLOUD_COVER_PCT, 50.0)::FLOAT AS ORIGIN_CLOUD_PCT,
    COALESCE(ORIGIN_WEATHER_CODE, 0)::INTEGER     AS ORIGIN_WX_CODE,
    COALESCE(DEST_TEMP_C, 15.0)::FLOAT       AS DEST_TEMP_C,
    COALESCE(DEST_WIND_SPEED_KMH, 10.0)::FLOAT    AS DEST_WIND_KMH,
    COALESCE(DEST_VISIBILITY_M, 10000.0)::FLOAT   AS DEST_VIS_M,
    COALESCE(DEST_WEATHER_CODE, 0)::INTEGER        AS DEST_WX_CODE,"""
    else:
        weather_cols = ""

    return f"""
SELECT
    FLOOR(SCHEDULED_DEP / 100)::INTEGER       AS DEP_HOUR,
    DAYOFWEEK(FLIGHT_DATE)::INTEGER            AS DEP_DOW,
    MONTH::INTEGER                             AS DEP_MONTH,
    COALESCE(DISTANCE, 0)::FLOAT              AS FLIGHT_DISTANCE,
    AIRLINE_CODE                               AS OP_CARRIER,
    ORIGIN,
    DEST,
    COALESCE(SCHEDULED_ELAPSED, 0)::FLOAT     AS SCHED_ELAPSED_MIN,
    {weather_cols}
    IS_DEP_DELAYED::INTEGER                   AS LABEL_CLASS,
    GREATEST(0, COALESCE(DEP_DELAY_MIN, 0))::FLOAT AS LABEL_REG
FROM STAGING.FLIGHTS {sample_clause}
WHERE IS_CANCELLED = FALSE
  AND FLIGHT_DATE >= '2000-01-01'
  AND FLIGHT_DATE < '2025-01-01'
"""


def fetch_data(sample_frac: float | None, use_weather: bool) -> pd.DataFrame:
    print("Connecting to Snowflake …")
    conn = get_conn(schema="STAGING")
    try:
        cur = conn.cursor()
        cur.execute("ALTER WAREHOUSE FLIGHT_WH RESUME IF SUSPENDED")
        cur.execute("USE DATABASE FLIGHT_DB")
        query = build_query(sample_frac, use_weather)
        print("Executing query (this may take a while for large samples) …")
        cur.execute(query)
        df = cur.fetch_pandas_all()
        print(f"Fetched {len(df):,} rows.")
        return df
    finally:
        cur.close()
        conn.close()


def build_preprocessor(feature_names: list[str], y_class: np.ndarray) -> ColumnTransformer:
    cat_cols  = [f for f in CATEGORICAL_FEATURES if f in feature_names]
    num_cols  = [f for f in feature_names if f not in cat_cols]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", "passthrough", num_cols),
            ("cat", TargetEncoder(target_type="continuous", random_state=42), cat_cols),
        ],
        remainder="drop",
    )
    return preprocessor


def train(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    use_weather = not args.no_weather
    sample_frac = args.sample_frac

    # ── 1. Fetch data ─────────────────────────────────────────────────────────
    try:
        df = fetch_data(sample_frac, use_weather)
    except Exception as exc:
        print(f"ERROR: Could not fetch data from Snowflake: {exc}")
        sys.exit(1)

    if df.empty:
        print("WARNING: STAGING.FLIGHTS returned 0 rows. Nothing to train.")
        sys.exit(0)

    # ── 2. Validate columns ───────────────────────────────────────────────────
    # Drop rows with null labels
    df = df.dropna(subset=[LABEL_CLASS, LABEL_REG])
    if df.empty:
        print("WARNING: All rows dropped after removing null labels. Exiting.")
        sys.exit(0)

    # If weather cols are missing (no data), fall back to no-weather mode
    if use_weather:
        missing_wx = [c for c in WEATHER_FEATURES if c not in df.columns]
        if missing_wx:
            print(f"WARNING: Weather columns missing: {missing_wx}. Falling back to no-weather mode.")
            use_weather = False

    # ── 3. Build feature list ─────────────────────────────────────────────────
    feature_names = NUMERIC_FEATURES[:]
    if use_weather:
        feature_names += WEATHER_FEATURES
    feature_names += CATEGORICAL_FEATURES

    # Keep only available columns
    feature_names = [f for f in feature_names if f in df.columns]
    X = df[feature_names].copy()
    y_class = df[LABEL_CLASS].values.astype(int)
    y_reg   = df[LABEL_REG].values.astype(float)
    y_reg   = np.clip(y_reg, 0, 300)

    print(f"Features: {feature_names}")
    print(f"Positive class rate: {y_class.mean():.3f}  ({y_class.sum():,} delayed / {len(y_class):,} total)")

    # ── 4. Train/val split ────────────────────────────────────────────────────
    X_train, X_val, yc_train, yc_val, yr_train, yr_val = train_test_split(
        X, y_class, y_reg,
        test_size=0.2, random_state=42, stratify=y_class
    )
    print(f"Train: {len(X_train):,}  |  Val: {len(X_val):,}")

    # ── 5. Build preprocessing ────────────────────────────────────────────────
    cat_cols = [f for f in CATEGORICAL_FEATURES if f in feature_names]
    num_cols = [f for f in feature_names if f not in cat_cols]

    preprocessor_clf = ColumnTransformer(
        transformers=[
            ("num", "passthrough", num_cols),
            ("cat", TargetEncoder(target_type="continuous", random_state=42), cat_cols),
        ],
        remainder="drop",
    )

    preprocessor_reg = ColumnTransformer(
        transformers=[
            ("num", "passthrough", num_cols),
            ("cat", TargetEncoder(target_type="continuous", random_state=42), cat_cols),
        ],
        remainder="drop",
    )

    # ── 6. Compute class weight ───────────────────────────────────────────────
    neg = int((yc_train == 0).sum())
    pos = int((yc_train == 1).sum())
    scale_pos = neg / max(pos, 1)
    print(f"scale_pos_weight = {scale_pos:.2f}")

    # ── 7. Build pipelines ────────────────────────────────────────────────────
    clf_pipeline = Pipeline([
        ("prep", preprocessor_clf),
        ("model", xgb.XGBClassifier(
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=5,
            scale_pos_weight=scale_pos,
            eval_metric="auc",
            early_stopping_rounds=30,
            tree_method="hist",
            random_state=42,
            verbosity=1,
        )),
    ])

    reg_pipeline = Pipeline([
        ("prep", preprocessor_reg),
        ("model", xgb.XGBRegressor(
            n_estimators=300,
            max_depth=7,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            objective="reg:squarederror",
            eval_metric="rmse",
            early_stopping_rounds=30,
            tree_method="hist",
            random_state=42,
            verbosity=1,
        )),
    ])

    # ── 8. Fit preprocessor + model ───────────────────────────────────────────
    # For early stopping we need to pass eval_set through the pipeline.
    # Fit the preprocessor first, then fit XGB with a transformed eval set.
    print("\nFitting classifier …")
    prep_clf = preprocessor_clf
    X_train_t = prep_clf.fit_transform(X_train, yc_train)
    X_val_t   = prep_clf.transform(X_val)
    clf_model = clf_pipeline.named_steps["model"]
    clf_model.fit(
        X_train_t, yc_train,
        eval_set=[(X_val_t, yc_val)],
        verbose=50,
    )
    # Rebuild pipeline with already-fit steps (avoid double-fit)
    clf_pipeline = Pipeline([
        ("prep", prep_clf),
        ("model", clf_model),
    ])

    print("\nFitting regressor …")
    prep_reg = preprocessor_reg
    X_train_r = prep_reg.fit_transform(X_train, yr_train)
    X_val_r   = prep_reg.transform(X_val)
    reg_model = reg_pipeline.named_steps["model"]
    reg_model.fit(
        X_train_r, yr_train,
        eval_set=[(X_val_r, yr_val)],
        verbose=50,
    )
    reg_pipeline = Pipeline([
        ("prep", prep_reg),
        ("model", reg_model),
    ])

    # ── 9. Evaluate ───────────────────────────────────────────────────────────
    print("\nEvaluating …")
    yc_pred  = clf_model.predict(X_val_t)
    yc_prob  = clf_model.predict_proba(X_val_t)[:, 1]
    yr_pred  = reg_model.predict(X_val_r)

    acc    = float(accuracy_score(yc_val, yc_pred))
    f1     = float(f1_score(yc_val, yc_pred, zero_division=0))
    auc    = float(roc_auc_score(yc_val, yc_prob))
    rmse   = float(np.sqrt(mean_squared_error(yr_val, yr_pred)))
    mae    = float(mean_absolute_error(yr_val, yr_pred))

    print(f"Classifier  — Accuracy: {acc:.4f}  F1: {f1:.4f}  AUC-ROC: {auc:.4f}")
    print(f"Regressor   — RMSE: {rmse:.2f} min  MAE: {mae:.2f} min")

    # ── 10. Collect label encoder info (unique values for dropdowns) ──────────
    label_encoders: dict[str, list] = {}
    for col in cat_cols:
        uniq = sorted(df[col].dropna().unique().tolist())
        label_encoders[col] = [str(v) for v in uniq]

    # ── 11. Save artifacts ────────────────────────────────────────────────────
    print(f"\nSaving artifacts to {output_dir} …")

    joblib.dump(clf_pipeline, output_dir / "delay_classifier.joblib")
    joblib.dump(reg_pipeline, output_dir / "delay_regressor.joblib")

    with open(output_dir / "feature_names.json", "w") as f:
        json.dump(feature_names, f, indent=2)

    with open(output_dir / "label_encoders.json", "w") as f:
        json.dump(label_encoders, f, indent=2)

    metrics = {
        "classifier": {
            "accuracy": round(acc, 6),
            "f1": round(f1, 6),
            "auc_roc": round(auc, 6),
            "val_rows": int(len(yc_val)),
        },
        "regressor": {
            "rmse": round(rmse, 4),
            "mae": round(mae, 4),
            "val_rows": int(len(yr_val)),
        },
        "feature_names": feature_names,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "training_rows": int(len(X_train)),
        "sample_frac": sample_frac if sample_frac else 1.0,
        "use_weather": use_weather,
    }
    with open(output_dir / "model_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print("Done. Artifacts written:")
    for name in [
        "delay_classifier.joblib",
        "delay_regressor.joblib",
        "feature_names.json",
        "label_encoders.json",
        "model_metrics.json",
    ]:
        print(f"  {output_dir / name}")


# ── CLI ───────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train XGBoost flight delay models on BTS/Snowflake data."
    )
    parser.add_argument(
        "--sample-frac", type=float, default=None,
        metavar="FRAC",
        help="Fraction of rows to sample (e.g. 0.1 for 10%%). Default: full dataset.",
    )
    parser.add_argument(
        "--no-weather", action="store_true",
        help="Skip weather features; use only schedule + carrier + airport.",
    )
    parser.add_argument(
        "--output-dir", type=str, default=str(Path(__file__).parent),
        help="Directory to write model artifacts. Default: same dir as this script.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    train(parse_args())
