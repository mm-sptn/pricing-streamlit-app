from utils.session import get_cached_session
import utils.get_data as gd
from snowflake.snowpark.functions import sproc, col, when
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

vendor_df = session.table('edl.phq.vendor_master').select(
    col('v_id').alias('vm_v_id'),
    col('vendor'),
    col('vendor_name')
)

im_df = session.table('edl.phq.item_master').select(
    col('item_id').alias('im_item_id'),
    col('upc_ean')
)

item_df = session.table('sbx_biz.marketing.t_item').select(
    col('"Product UPC"').alias('product_upc'),
    col('"Unit Size"').alias('unit_size'),
    col('"Item Description"').alias('item_description'),
    col('"Brand"').alias('brand'),
    col('"Group ID"').alias('group_id'),
    col('"Category ID"').alias('category_id'),
    col('"Anchor Group ID"').alias('anchor_group_id')
)

current_zone_key = price_zones_dict.get(current_zone_label)
target_zone_key = price_zones_dict.get(target_zone_label)

current_reg_item_prices_df = gd.get_reg_item_prices(eff_date, current_zone_key)
target_reg_item_prices_df = gd.get_reg_item_prices(eff_date, target_zone_key)

current_promo_item_prices_df = gd.get_promo_item_prices(eff_date, current_zone_key)
target_promo_item_prices_df = gd.get_promo_item_prices(eff_date, target_zone_key)

current_mvmt_df = gd.get_26w_movement(eff_date, current_zone_key)
target_mvmt_df = gd.get_26w_movement(eff_date, target_zone_key)

diff_reg_item_prices_df = current_reg_item_prices_df.join(
    target_reg_item_prices_df,
    on = 
        (current_reg_item_prices_df['item_id'] == target_reg_item_prices_df['item_id']) &
        (current_reg_item_prices_df['v_id'] == target_reg_item_prices_df['v_id']),
    how = 'fullouter'
    ).filter(
        current_reg_item_prices_df['unit_price'] != target_reg_item_prices_df['unit_price']
    ).select(
        current_reg_item_prices_df['item_id'].alias('joined_item_id'),
        current_reg_item_prices_df['v_id'].alias('joined_v_id'),
        current_reg_item_prices_df['zonename'].alias('moving_zone'),
        current_reg_item_prices_df['price_strategy'].alias('moving_price_strategy'),
        current_reg_item_prices_df['start_date'].alias('moving_start'),
        current_reg_item_prices_df['end_date'].alias('moving_end'),
        current_reg_item_prices_df['price_multiple'].alias('moving_multiple'),
        current_reg_item_prices_df['unit_price'].alias('moving_retail'),
        target_reg_item_prices_df['price_strategy'].alias('target_price_strategy'),
        target_reg_item_prices_df['zonename'].alias('target_zone'),
        target_reg_item_prices_df['start_date'].alias('target_start'),
        target_reg_item_prices_df['end_date'].alias('target_end'),
        target_reg_item_prices_df['price_multiple'].alias('target_multiple'),
        target_reg_item_prices_df['unit_price'].alias('target_retail')
    )

df = diff_reg_item_prices_df.join(
    current_promo_item_prices_df,
    on = 
        (diff_reg_item_prices_df['joined_item_id'] == current_promo_item_prices_df['item_id']) &
        (diff_reg_item_prices_df['joined_v_id'] == current_promo_item_prices_df['v_id']),
    how = 'left'
).join(
    target_promo_item_prices_df,
    on = 
        (diff_reg_item_prices_df['joined_item_id'] == target_promo_item_prices_df['item_id']) &
        (diff_reg_item_prices_df['joined_v_id'] == target_promo_item_prices_df['v_id']),
    how = 'left'
).join(
    current_mvmt_df,
    on =
        (diff_reg_item_prices_df['moving_price_strategy'] == current_mvmt_df['zonecode']) &
        (diff_reg_item_prices_df['joined_item_id'] == current_mvmt_df['sales_item_id']),
    how = 'left'
).join(
    target_mvmt_df,
    on =
        (diff_reg_item_prices_df['moving_price_strategy'] == target_mvmt_df['zonecode']) &
        (diff_reg_item_prices_df['joined_item_id'] == target_mvmt_df['sales_item_id']),
    how = 'left'
).join(
    vendor_df,
    diff_reg_item_prices_df['joined_v_id'] == vendor_df['vm_v_id'],
    how = 'left'
).join(
    im_df,
    diff_reg_item_prices_df['joined_item_id'] == im_df['im_item_id'],
    how = 'left'
).join(
    item_df,
    im_df['upc_ean'] == item_df['product_upc'],
    how = 'left'
).select(
    item_df['product_upc'].alias('UPC'),
    item_df['item_description'].alias('Item Description'),
    item_df['unit_size'].alias('Unit Size'),
    item_df['anchor_group_id'].alias('Anchor Group ID'),
    item_df['brand'].alias('Brand'),
    item_df['group_id'].alias('Group ID'),
    item_df['category_id'].alias('Category ID'),
    vendor_df['vendor'].alias('Vendor Number'),
    vendor_df['vendor_name'].alias('Vendor'),
    diff_reg_item_prices_df['moving_zone'].alias('Moving Zone'),
    diff_reg_item_prices_df['moving_start'].alias('M From'),
    diff_reg_item_prices_df['moving_end'].alias('M Through'),
    diff_reg_item_prices_df['moving_multiple'].alias('M Multiple'),
    diff_reg_item_prices_df['moving_retail'].alias('M Retail'),
    current_mvmt_df['26w_mvmt'].alias('M 26w Mvmt'),
    diff_reg_item_prices_df['target_zone'].alias('Target Zone'),
    diff_reg_item_prices_df['target_start'].alias('T From'),
    diff_reg_item_prices_df['target_end'].alias('T Through'),
    diff_reg_item_prices_df['target_multiple'].alias('T Multiple'),
    diff_reg_item_prices_df['target_retail'].alias('T Retail'),
    target_mvmt_df['26w_mvmt'].alias('T 26w Mvmt'),
    current_promo_item_prices_df['description'].alias('M Promo'),
    current_promo_item_prices_df['start_date'].alias('M Promo From'),
    current_promo_item_prices_df['end_date'].alias('M Promo Through'),
    current_promo_item_prices_df['price_multiple'].alias('M Promo Multiple'),
    current_promo_item_prices_df['unit_price'].alias('M Promo Retail'),
    target_promo_item_prices_df['description'].alias('T Promo'),
    target_promo_item_prices_df['start_date'].alias('T Promo From'),
    target_promo_item_prices_df['end_date'].alias('T Promo Through'),
    target_promo_item_prices_df['price_multiple'].alias('T Promo Multiple'),
    target_promo_item_prices_df['unit_price'].alias('T Promo Retail')
)

st.write(df)
st.write(f"Total Items: {df.count()}")

