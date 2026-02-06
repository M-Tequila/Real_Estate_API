import requests
import streamlit as st
import pandas as pd

# -----------------------------
# Config
# -----------------------------
API_BASE_URL = "https://real-estate-api-sgdz.onrender.com"
st.set_page_config(page_title="Nigeria Real Estate Dashboard", layout="wide")


# -----------------------------
# Helpers
# -----------------------------
@st.cache_data(ttl=3600)
def get_all_data_sample():
    """
    Pull a broad sample to extract filter values.
    This assumes the API returns enough rows for distinct values.
    """
    response = requests.get(f"{API_BASE_URL}/api/average_price", timeout=10)
    if response.status_code == 200:
        return response.json()
    return None


def get_average_price(state=None, property_type=None):
    params = {}
    if state:
        params["state"] = state
    if property_type:
        params["property_type"] = property_type

    response = requests.get(
        f"{API_BASE_URL}/api/average_price",
        params=params,
        timeout=10
    )

    if response.status_code == 200:
        return response.json(), None

    if response.status_code in [400, 404]:
        return None, response.json().get("detail")

    return None, "Unexpected API error."


def get_trends(state=None, property_type=None):
    params = {}
    if state:
        params["state"] = state
    if property_type:
        params["property_type"] = property_type

    response = requests.get(
        f"{API_BASE_URL}/api/trends",
        params=params,
        timeout=10
    )

    if response.status_code == 200:
        return pd.DataFrame(response.json()), None

    if response.status_code in [400, 404]:
        return pd.DataFrame(), response.json().get("detail")

    return pd.DataFrame(), "Unexpected API error."


# -----------------------------
# Sidebar Filters
# -----------------------------
st.sidebar.header("🔍 Filters")

# Because API has no metadata endpoint, define safe fallbacks
STATE_OPTIONS = ["Lagos", "Abuja", "Oyo", "Rivers", "Ogun", "Delta"]
PROPERTY_TYPE_OPTIONS = [
    "flat/apartment",
    "house",
    "land",
    "semi detached duplex",
    "fully detached",
    "terraced duplex"
]

selected_state = st.sidebar.selectbox(
    "Select State",
    ["All"] + STATE_OPTIONS
)

selected_property_type = st.sidebar.selectbox(
    "Select Property Type",
    ["All"] + PROPERTY_TYPE_OPTIONS
)

state_param = None if selected_state == "All" else selected_state
type_param = None if selected_property_type == "All" else selected_property_type


# -----------------------------
# Main Dashboard
# -----------------------------
st.title("🏡 Nigeria Real Estate Market Intelligence")
st.caption(
    "Powered by cleaned multi-source property listings. "
    "Some filters may not yet have enough historical depth."
)

# -----------------------------
# Average Price Metric
# -----------------------------
result, error = get_average_price(
    state=state_param,
    property_type=type_param
)

col1, col2 = st.columns(2)

with col1:
    if result and result.get("average_price"):
        st.metric(
            label="Average Property Price",
            value=f"₦{result['average_price']:,.0f}",
            delta=f"{result['count']} listings"
        )
    else:
        st.warning(error or "No data available for this selection.")

# -----------------------------
# Monthly Trends Chart
# -----------------------------
with col2:
    st.subheader("📈 Monthly Price Trends")

    trends_df, trend_error = get_trends(
        state=state_param,
        property_type=type_param
    )

    if not trends_df.empty:
        st.line_chart(
            trends_df.set_index("month")["average_price"]
        )
    else:
        st.info(trend_error or "Not enough data to display trends yet.")

# -----------------------------
# UX Note
# -----------------------------
st.markdown(
    """
    **ℹ️ Data notes**
    - Trends require a minimum sample size per month.
    - Some city–property combinations are still building history.
    - Values update as more listings are scraped.
    """
)