import pandas as pd
import os
import re
from fastapi import FastAPI, HTTPException

# -----------------------------
# LOAD DATA
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "Final_data_CSV.csv")

df = pd.read_csv(DATA_PATH, encoding="latin1")

# -----------------------------
# CLEAN PRICE
# -----------------------------
def clean_price(value):
    if pd.isna(value):
        return None

    value = str(value).lower().strip()

    # remove rentals / shortlets
    if any(x in value for x in ["rent", "shortlet", "per", "month", "day"]):
        return None

    # handle millions (e.g 3.5m)
    if "m" in value:
        num = re.findall(r"\d+\.?\d*", value)
        if num:
            return float(num[0]) * 1_000_000

    # extract numeric values
    num = re.findall(r"\d+", value.replace(",", ""))
    if num:
        return float("".join(num))

    return None


df["price"] = df["price"].apply(clean_price)

# -----------------------------
# REMOVE INVALID PRICES
# -----------------------------
df = df[df["price"].notna()]
df = df[df["price"] > 100_000]

# -----------------------------
# CLEAN PROPERTY TYPE (NEW COLUMN ONLY)
# -----------------------------
def normalize_property_type(x):
    if pd.isna(x):
        return "unknown"

    x = str(x).lower()

    if "flat" in x or "apartment" in x:
        return "flat/apartment"
    elif "duplex" in x:
        return "duplex"
    elif "terrace" in x:
        return "terraced duplex"
    elif "semi" in x:
        return "semi detached duplex"
    elif "detached" in x:
        return "fully detached"
    elif "bungalow" in x:
        return "bungalow"
    elif "land" in x or "plot" in x:
        return "land"
    elif "house" in x:
        return "house"

    return "other"


# ✅ DO NOT overwrite original column
df["property_type_clean"] = df["property_type"].apply(normalize_property_type)

# -----------------------------
# STATE HANDLING (SAFE)
# -----------------------------
states = [
    "lagos","abuja","oyo","rivers","ogun","delta","edo","kaduna","kano",
    "plateau","enugu","imo","anambra","kwara","osun","ondo","ekiti",
    "kogi","niger","bauchi","gombe","taraba","adamawa","borno","yobe",
    "katsina","zamfara","sokoto","kebbi","cross river","akwa ibom",
    "bayelsa","ebonyi","nasarawa","benue","jigawa"
]

def extract_state(location):
    if pd.isna(location):
        return "Unknown"

    location = location.lower()

    for state in states:
        if state in location:
            return state.title()

    return "Unknown"


if "state" not in df.columns or df["state"].isna().sum() > 0:
    df["state"] = df["location"].apply(extract_state)

# -----------------------------
# DATE CLEANING
# -----------------------------
df["added_date"] = pd.to_datetime(df["added_date"], errors="coerce", dayfirst=True)
df["month_added"] = df["added_date"].dt.to_period("M").dt.to_timestamp()

# -----------------------------
# GLOBAL OUTLIER FILTER
# -----------------------------
df = df[(df["price"] >= 500_000) & (df["price"] <= 1_500_000_000)]

# -----------------------------
# REMOVE DUPLICATES
# -----------------------------
df = df.drop_duplicates()

# -----------------------------
# FASTAPI APP
# -----------------------------
app = FastAPI(title="Real Estate API")

MIN_SAMPLE_SIZE = 5

VALID_PROPERTY_CATEGORIES = sorted(
    df["property_type_clean"].dropna().unique().tolist()
)

# -----------------------------
# ROOT
# -----------------------------
@app.get("/")
def home():
    return {
        "message": "Real Estate API is running 🚀",
        "rows_loaded": int(len(df))
    }

# -----------------------------
# FILTER FUNCTION (USES CLEAN COLUMN)
# -----------------------------
def filter_data(data, state=None, property_type=None):
    if state:
        data = data[data["state"].str.lower() == state.lower()]

    if property_type:
        if property_type.lower() not in [c.lower() for c in VALID_PROPERTY_CATEGORIES]:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid property_type. Allowed values: {VALID_PROPERTY_CATEGORIES}"
            )

        data = data[
            data["property_type_clean"].str.lower() == property_type.lower()
        ]

    return data

# -----------------------------
# MEDIAN PRICE
# -----------------------------
@app.get("/api/average_price")
def average_price(state: str | None = None, property_type: str | None = None):

    data = filter_data(df, state, property_type)

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

# -----------------------------
# PRICE TRENDS
# -----------------------------
@app.get("/api/trends")
def price_trends(state: str | None = None, property_type: str | None = None):

    if not state and not property_type:
        raise HTTPException(
            status_code=400,
            detail="Please provide at least a state or property_type."
        )

    data = filter_data(df, state, property_type)

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
