import pandas as pd
import os
import re
from fastapi import FastAPI, HTTPException

#Load Data

df = pd.read_csv(r"final_data.csv")


#clean price
def clean_price(value):
    if pd.isna(value):
        return None

    value = str(value).lower()

    #convert shorthand e.g 3.5m
    if "m" in value:
        num = re.findall(r"\d+\.?\d*", value)
        if num:
            return float(num[0]) * 1_000_000

    #remove everything except digits
    num = re.findall(r"\d+", value.replace(",", ""))
    if num:
        return float("".join(num))

    return None


df["price"] = df["price"].apply(clean_price)

#remove invalid values
df = df[df["price"].notna()]
df = df[df["price"] > 0]

#date cleaning
df["added_date"] = pd.to_datetime(df["added_date"], errors="coerce", dayfirst=True)
df["updated_date"] = pd.to_datetime(df["updated_date"], errors="coerce", dayfirst=True)

df["month_added"] = df["added_date"].dt.to_period("M").dt.to_timestamp()

#REMOVE EXTREME OUTLIERS
#Nigeria market realistic band
df = df[(df["price"] >= 5_000_000) & (df["price"] <= 1_000_000_000)]

#FastAPI App
app = FastAPI(title="Real Estate API")

MIN_SAMPLE_SIZE = 10

VALID_PROPERTY_CATEGORIES = sorted(
    df["property_type"].dropna().unique().tolist()
)

#ROOT
@app.get("/")
def home():
    return {
        "message": "Real Estate API is running 🚀",
        "rows_loaded": int(len(df))
    }

# SAFE FILTER FUNCTION
def filter_data(data, state=None, property_type=None):
    if state:
        data = data[data["state"].str.lower() == state.lower()]

    if property_type:
        if property_type.lower() not in [c.lower() for c in VALID_PROPERTY_CATEGORIES]:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid property_type. Allowed values: {VALID_PROPERTY_CATEGORIES}"
            )

        data = data[data["property_type"].str.lower() == property_type.lower()]

    return data

#remove remaining outliers(IQR)
def remove_outliers(data):
    Q1 = data["price"].quantile(0.25)
    Q3 = data["price"].quantile(0.75)
    IQR = Q3 - Q1

    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR

    return data[(data["price"] >= lower) & (data["price"] <= upper)]

#average price(median)
@app.get("/api/average_price")
def average_price(state: str | None = None, property_type: str | None = None):

    data = filter_data(df.copy(), state, property_type)
    data = remove_outliers(data)

    if len(data) < MIN_SAMPLE_SIZE:
        raise HTTPException(
            status_code=404,
            detail="Not enough data to compute a reliable price."
        )

    return {
        "state": state,
        "property_type": property_type,
        "average_price": round(float(data["price"].median()), 2),
        "count": int(len(data))
    }

#price trends
@app.get("/api/trends")
def price_trends(state: str | None = None, property_type: str | None = None):

    if not state and not property_type:
        raise HTTPException(
            status_code=400,
            detail="Please provide at least a state or property_type."
        )

    data = filter_data(df.copy(), state, property_type)
    data = remove_outliers(data)

    if data.empty:
        raise HTTPException(
            status_code=404,
            detail="No data found for the selected filters."
        )

    trends = (
        data
        .groupby("month_added")
        .agg(
            average_price=("price", "median"),
            count=("price", "count")
        )
        .reset_index()
    )

    trends = trends[trends["count"] >= MIN_SAMPLE_SIZE]

    if trends.empty:
        raise HTTPException(
            status_code=404,
            detail="Not enough data to compute reliable trends."
        )

    return [
        {
            "month": row["month_added"].strftime("%Y-%m"),
            "average_price": round(float(row["average_price"]), 2),
            "count": int(row["count"])
        }
        for _, row in trends.iterrows()
    ]
