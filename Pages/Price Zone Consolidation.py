from utils.session import get_cached_session
import utils.get_data as gd
from snowflake.snowpark.functions import sproc, col
import streamlit as st
from datetime import date, timedelta

session = get_cached_session()

today = date.today()
days_since_saturday = (today.weekday() - 5) % 7
default_date = today - timedelta(days=days_since_saturday)

zone_map_df = gd.get_price_strategies()
price_zones_dict = dict(zip(zone_map_df['ZONENAME'], zone_map_df['ZONECODE']))

eff_date = st.sidebar.date_input(
    label = "Effective Date",
    value=default_date,
    format="YYYY/MM/DD"
)

st.sidebar.subheader("Zone Selection")

current_zone_label = st.sidebar.selectbox(
    "Current Zone", 
    zone_map_df['ZONENAME'], 
    key = "current_zone"
)

target_zone_label = st.sidebar.selectbox(
    "Target Zone",
    zone_map_df['ZONENAME'], 
    key = "target_zone"
)

current_zone_key = price_zones_dict.get(current_zone_label)
target_zone_key = price_zones_dict.get(target_zone_label)

item_prices_df = gd.get_item_prices(eff_date, current_zone_key)

st.write(item_prices_df)
