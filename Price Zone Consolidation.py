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

current_item_prices_df = gd.get_item_prices(eff_date, current_zone_key)
target_item_prices_df = gd.get_item_prices(eff_date, target_zone_key)

diff_item_prices_df = current_item_prices_df.join(
    target_item_prices_df,
    on = current_item_prices_df['FULL_UPC_NBR'] == target_item_prices_df['FULL_UPC_NBR'],
    how = 'inner'
    ).filter(col("c.IP_UNIT_PRICE") != col("t.IP_UNIT_PRICE")).select(
    current_item_prices_df['"Anchor Group ID"'].alias('Anchor Group ID'),
    current_item_prices_df['FULL_UPC_NBR'].alias('UPC'),
    current_item_prices_df['IP_PRICE_MULTIPLE'].alias('C Multiple'),
    current_item_prices_df['IP_UNIT_PRICE'].alias('C Retail'),
    current_item_prices_df['IP_START_DATE'].alias('C From'),
    current_item_prices_df['IP_END_DATE'].alias('C Through'),
    target_item_prices_df['IP_PRICE_MULTIPLE'].alias('T Multiple'),
    target_item_prices_df['IP_UNIT_PRICE'].alias('T Retail'),
    target_item_prices_df['IP_START_DATE'].alias('T From'),
    target_item_prices_df['IP_END_DATE'].alias('T Through'),
    current_item_prices_df['PROMO_TYPE'].alias('Promo'),
    current_item_prices_df['PROMO_PRICE_MULTIPLE'].alias('Promo Multiple'),
    current_item_prices_df['PROMO_UNIT_PRICE'].alias('Promo Retail'),
    current_item_prices_df['PROMO_START_DATE'].alias('Promo From'),
    current_item_prices_df['PROMO_END_DATE'].alias('Promo Through'),
    current_item_prices_df['"Brand"'].alias('Brand'),
    current_item_prices_df['"Item Description"'].alias('Item Description'),
    current_item_prices_df['"Unit Size"'].alias('Unit Size'),
    current_item_prices_df['"Group ID"'].alias('Group ID'),
    current_item_prices_df['"Category ID"'].alias('Category ID'),
    current_item_prices_df['STORE_COUNT'].alias('C Store Count'),
    target_item_prices_df['STORE_COUNT'].alias('T Store Count')
)
diff_item_prices_df = diff_item_prices_df.to_pandas()
st.write(diff_item_prices_df)
st.write(f"Total Items: {len(diff_item_prices_df)}")