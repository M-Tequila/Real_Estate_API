from __future__ import annotations

import os
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "Final_data_CSV.csv")
STATE_SUMMARY_PATH = os.path.join(BASE_DIR, "Final_data_CSV.csv")

if not os.path.exists(DATA_PATH):
    raise FileNotFoundError(f"Dataset not found: {DATA_PATH}")


df = pd.read_csv(DATA_PATH)
state_summary_df = pd.read_csv(STATE_SUMMARY_PATH) if os.path.exists(STATE_SUMMARY_PATH) else pd.DataFrame()


# -------------------------
# Data cleaning / typing
# -------------------------
for col in ["price", "bedrooms", "bathrooms"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

df["added_date"] = pd.to_datetime(df["added_date"], errors="coerce", dayfirst=True)
df["updated_date"] = pd.to_datetime(df["updated_date"], errors="coerce", dayfirst=True)
df["month_posted"] = pd.to_datetime(df["month_posted"], errors="coerce", format="%Y-%m")
df["price_per_bedroom"] = df["price"] / df["bedrooms"]
df.loc[df["bedrooms"].isna() | (df["bedrooms"] <= 0), "price_per_bedroom"] = pd.NA


# -------------------------
# FastAPI app
# -------------------------
app = FastAPI(
    title="Nigeria Real Estate Analysis API",
    description="API endpoints for the cleaned, deduplicated, analysis-ready Nigeria real estate dataset.",
    version="1.0.0",
)


MIN_STATE_PROPERTY_SAMPLE = 5
MIN_AREA_SAMPLE = 5

VALID_STATES = sorted(df["state"].dropna().unique().tolist())
VALID_PROPERTY_TYPES = sorted(df["property_type"].dropna().unique().tolist())
VALID_AREA_BUCKETS = sorted(df["area_bucket"].dropna().unique().tolist())
VALID_PRICE_CATEGORIES = sorted(df["price_category"].dropna().unique().tolist())


def normalize_lookup(value: str | None) -> str | None:
    return value.strip().lower() if value else None


def validate_choice(value: str | None, allowed: list[str], label: str) -> None:
    if value is None:
        return
    allowed_lookup = {item.lower(): item for item in allowed}
    if value.lower() not in allowed_lookup:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {label}. Allowed values: {allowed}",
        )


def apply_filters(
    data: pd.DataFrame,
    state: str | None = None,
    property_type: str | None = None,
    area_bucket: str | None = None,
    price_category: str | None = None,
) -> pd.DataFrame:
    filtered = data.copy()

    validate_choice(state, VALID_STATES, "state")
    validate_choice(property_type, VALID_PROPERTY_TYPES, "property_type")
    validate_choice(area_bucket, VALID_AREA_BUCKETS, "area_bucket")
    validate_choice(price_category, VALID_PRICE_CATEGORIES, "price_category")

    if state:
        filtered = filtered[filtered["state"].str.lower() == state.lower()]
    if property_type:
        filtered = filtered[filtered["property_type"].str.lower() == property_type.lower()]
    if area_bucket:
        filtered = filtered[filtered["area_bucket"].str.lower() == area_bucket.lower()]
    if price_category:
        filtered = filtered[filtered["price_category"].str.lower() == price_category.lower()]

    return filtered


def enforce_state_property_reliability(
    data: pd.DataFrame,
    state: str | None,
    property_type: str | None,
) -> None:
    if state and property_type and len(data) < MIN_STATE_PROPERTY_SAMPLE:
        raise HTTPException(
            status_code=422,
            detail=(
                "There are not enough listings for this property type in this state for analysis."
            ),
        )


def serialize_records(data: pd.DataFrame, limit: int = 100) -> list[dict[str, Any]]:
    records = data.head(limit).copy()
    if "added_date" in records:
        records["added_date"] = records["added_date"].dt.strftime("%Y-%m-%d")
    if "updated_date" in records:
        records["updated_date"] = records["updated_date"].dt.strftime("%Y-%m-%d")
    if "month_posted" in records:
        records["month_posted"] = records["month_posted"].dt.strftime("%Y-%m")
    return records.where(pd.notnull(records), None).to_dict(orient="records")


# -------------------------
# Root / metadata
# -------------------------
@app.get("/")
def home() -> dict[str, Any]:
    return {
        "message": "Nigeria Real Estate Analysis API is running.",
        "dataset": os.path.basename(DATA_PATH),
        "rows_loaded": int(len(df)),
        "states_available": len(VALID_STATES),
        "property_types_available": len(VALID_PROPERTY_TYPES),
    }


@app.get("/api/metadata")
def metadata() -> dict[str, Any]:
    return {
        "states": VALID_STATES,
        "property_types": VALID_PROPERTY_TYPES,
        "area_buckets": VALID_AREA_BUCKETS,
        "price_categories": VALID_PRICE_CATEGORIES,
        "rules": {
            "state_property_minimum": MIN_STATE_PROPERTY_SAMPLE,
            "area_minimum": MIN_AREA_SAMPLE,
            "central_tendency": "median",
            "price_per_bedroom_requires_bedrooms_gt_0": True,
        },
    }


# -------------------------
# Summary metrics
# -------------------------
@app.get("/api/summary")
def summary(
    state: str | None = None,
    property_type: str | None = None,
    area_bucket: str | None = None,
    price_category: str | None = None,
) -> dict[str, Any]:
    data = apply_filters(df, state, property_type, area_bucket, price_category)

    if data.empty:
        raise HTTPException(status_code=404, detail="No data found for the selected filters.")

    enforce_state_property_reliability(data, state, property_type)

    bedroom_df = data[data["bedrooms"].fillna(0) > 0]

    return {
        "filters": {
            "state": state,
            "property_type": property_type,
            "area_bucket": area_bucket,
            "price_category": price_category,
        },
        "listing_count": int(len(data)),
        "median_price": round(float(data["price"].median()), 2),
        "min_price": round(float(data["price"].min()), 2),
        "max_price": round(float(data["price"].max()), 2),
        "median_price_per_bedroom": (
            round(float(bedroom_df["price_per_bedroom"].median()), 2)
            if not bedroom_df.empty
            else None
        ),
        "bedroom_valid_listing_count": int(len(bedroom_df)),
    }


# -------------------------
# State pricing
# -------------------------
@app.get("/api/state-pricing")
def state_pricing(
    property_type: str | None = None,
    area_bucket: str | None = None,
) -> list[dict[str, Any]]:
    data = apply_filters(df, None, property_type, area_bucket, None)

    if data.empty:
        raise HTTPException(status_code=404, detail="No data found for the selected filters.")

    grouped = (
        data.groupby("state", as_index=False)
        .agg(
            listings=("price", "size"),
            median_price=("price", "median"),
            min_price=("price", "min"),
            max_price=("price", "max"),
        )
        .sort_values("median_price", ascending=False)
    )

    return [
        {
            "state": row["state"],
            "listings": int(row["listings"]),
            "median_price": round(float(row["median_price"]), 2),
            "min_price": round(float(row["min_price"]), 2),
            "max_price": round(float(row["max_price"]), 2),
        }
        for _, row in grouped.iterrows()
    ]


# -------------------------
# State property reliability
# -------------------------
@app.get("/api/reliability")
def reliability(
    state: str | None = None,
    property_type: str | None = None,
) -> list[dict[str, Any]] | dict[str, Any]:
    data = apply_filters(df, state, property_type, None, None)

    if data.empty:
        raise HTTPException(status_code=404, detail="No data found for the selected filters.")

    grouped = (
        data.groupby(["state", "property_type"], as_index=False)
        .agg(
            listings=("price", "size"),
            median_price=("price", "median"),
        )
        .sort_values(["listings", "state", "property_type"], ascending=[True, True, True])
    )

    response = [
        {
            "state": row["state"],
            "property_type": row["property_type"],
            "listings": int(row["listings"]),
            "median_price": round(float(row["median_price"]), 2),
            "sufficient_for_analysis": bool(row["listings"] >= MIN_STATE_PROPERTY_SAMPLE),
            "message": (
                "Sufficient"
                if row["listings"] >= MIN_STATE_PROPERTY_SAMPLE
                else "There are not enough listings for this property type in this state for analysis."
            ),
        }
        for _, row in grouped.iterrows()
    ]

    if state and property_type:
        return response[0]
    return response


# -------------------------
# Monthly trends
# -------------------------
@app.get("/api/trends")
def price_trends(
    state: str | None = None,
    property_type: str | None = None,
    area_bucket: str | None = None,
) -> list[dict[str, Any]]:
    data = apply_filters(df, state, property_type, area_bucket, None)

    if data.empty:
        raise HTTPException(status_code=404, detail="No data found for the selected filters.")

    enforce_state_property_reliability(data, state, property_type)

    trends = (
        data.dropna(subset=["month_posted"])
        .groupby("month_posted", as_index=False)
        .agg(
            median_price=("price", "median"),
            listings=("price", "size"),
        )
        .sort_values("month_posted")
    )

    if trends.empty:
        raise HTTPException(status_code=404, detail="No trend data available for the selected filters.")

    return [
        {
            "month": row["month_posted"].strftime("%Y-%m"),
            "median_price": round(float(row["median_price"]), 2),
            "listings": int(row["listings"]),
        }
        for _, row in trends.iterrows()
    ]


# -------------------------
# Price per bedroom
# -------------------------
@app.get("/api/price-per-bedroom")
def price_per_bedroom(
    state: str | None = None,
    property_type: str | None = None,
    area_bucket: str | None = None,
) -> dict[str, Any]:
    data = apply_filters(df, state, property_type, area_bucket, None)
    data = data[data["bedrooms"].fillna(0) > 0]

    if data.empty:
        raise HTTPException(
            status_code=404,
            detail="No listings with valid bedroom counts were found for the selected filters.",
        )

    enforce_state_property_reliability(data, state, property_type)

    return {
        "filters": {
            "state": state,
            "property_type": property_type,
            "area_bucket": area_bucket,
        },
        "listing_count": int(len(data)),
        "median_price_per_bedroom": round(float(data["price_per_bedroom"].median()), 2),
        "median_price": round(float(data["price"].median()), 2),
        "median_bedrooms": round(float(data["bedrooms"].median()), 2),
    }


# -------------------------
# Top expensive areas
# -------------------------
@app.get("/api/top-areas")
def top_areas(
    state: str | None = None,
    property_type: str | None = None,
    limit: int = Query(default=10, ge=1, le=50),
) -> list[dict[str, Any]]:
    data = apply_filters(df, state, property_type, None, None)

    if data.empty:
        raise HTTPException(status_code=404, detail="No data found for the selected filters.")

    grouped = (
        data.groupby(["state", "area_bucket"], as_index=False)
        .agg(
            listings=("price", "size"),
            median_price=("price", "median"),
            min_price=("price", "min"),
            max_price=("price", "max"),
        )
    )
    grouped = grouped[grouped["listings"] >= MIN_AREA_SAMPLE].sort_values("median_price", ascending=False).head(limit)

    if grouped.empty:
        raise HTTPException(
            status_code=404,
            detail="No area buckets with enough listings were found for the selected filters.",
        )

    return [
        {
            "state": row["state"],
            "area_bucket": row["area_bucket"],
            "listings": int(row["listings"]),
            "median_price": round(float(row["median_price"]), 2),
            "min_price": round(float(row["min_price"]), 2),
            "max_price": round(float(row["max_price"]), 2),
        }
        for _, row in grouped.iterrows()
    ]


# -------------------------
# Records preview
# -------------------------
@app.get("/api/listings")
def listings(
    state: str | None = None,
    property_type: str | None = None,
    area_bucket: str | None = None,
    price_category: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    data = apply_filters(df, state, property_type, area_bucket, price_category)

    if data.empty:
        raise HTTPException(status_code=404, detail="No data found for the selected filters.")

    return {
        "count": int(len(data)),
        "returned": min(limit, len(data)),
        "records": serialize_records(
            data[
                [
                    "title",
                    "location",
                    "area_bucket",
                    "property_type",
                    "price",
                    "bedrooms",
                    "bathrooms",
                    "added_date",
                    "updated_date",
                    "month_posted",
                    "state",
                    "price_category",
                ]
            ],
            limit=limit,
        ),
    }
