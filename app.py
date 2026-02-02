import pandas as pd
import os
from fastapi import FastAPI, HTTPException


#Load dataset safely (works locally + Render)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "new_cleaned_properties.csv")

df = pd.read_csv(DATA_PATH)

# Ensure date columns are datetime
df["added_date"] = pd.to_datetime(df["added_date"], errors="coerce")
df["updated_date"] = pd.to_datetime(df["updated_date"], errors="coerce")

#Ensure month_added exists
if "month_added" not in df.columns:
    df["month_added"] = df["added_date"].dt.to_period("M").astype(str)

app = FastAPI()

MIN_SAMPLE_SIZE = 10

VALID_PROPERTY_CATEGORIES = sorted(
    df["property_type"].dropna().unique().tolist()
)

#Root
@app.get("/")
def home():
    return {
        "message": "Real Estate API",
        "rows_loaded": len(df)
    }

#Average Price
@app.get("/api/average_price")
def average_price(
    state: str | None = None,
    property_type: str | None = None
):
    data = df.copy()

    if state:
        data = data[data["state"].str.lower() == state.lower()]

    if property_type:
        if property_type.lower() not in [c.lower() for c in VALID_PROPERTY_CATEGORIES]:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid property_type. Allowed values: {VALID_PROPERTY_CATEGORIES}"
            )

        data = data[data["property_type"].str.lower() == property_type.lower()]

    if data.empty or len(data) < MIN_SAMPLE_SIZE:
        raise HTTPException(
            status_code=404,
            detail="Not enough data to compute a reliable average price."
        )

    return {
        "state": state,
        "property_type": property_type,
        "average_price": round(data["price"].mean(), 2),
        "count": len(data)
    }

#Trends

@app.get("/api/trends")
def price_trends(
    state: str | None = None,
    property_type: str | None = None
):
    if not state and not property_type:
        raise HTTPException(
            status_code=400,
            detail="Please provide at least a state or property_type to view trends."
        )

    data = df.copy()

    if state:
        data = data[data["state"].str.lower() == state.lower()]

    if property_type:
        if property_type.lower() not in [c.lower() for c in VALID_PROPERTY_CATEGORIES]:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid property_type. Allowed values: {VALID_PROPERTY_CATEGORIES}"
            )

        data = data[data["property_type"].str.lower() == property_type.lower()]

    if data.empty:
        raise HTTPException(
            status_code=404,
            detail="No data found for the selected filters."
        )

    trends = (
        data
        .groupby("month_added")
        .agg(
            average_price=("price", "mean"),
            count=("price", "count")
        )
        .reset_index()
    )

    trends = trends[trends["count"] >= MIN_SAMPLE_SIZE]

    if trends.empty:
        raise HTTPException(
            status_code=404,
            detail="Not enough data to compute reliable trends for this selection."
        )

    return [
        {
            "month": row["month_added"],
            "average_price": round(row["average_price"], 2),
            "count": int(row["count"])
        }
        for _, row in trends.iterrows()
    ]
