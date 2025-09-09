from utils.session import get_cached_session
import utils.get_data as gd
from snowflake.snowpark.functions import col, when, coalesce, to_date, lit, concat
import streamlit as st
import pandas as pd
from datetime import date, timedelta
import seaborn as sns
import matplotlib.pyplot as plt
import streamlit as st
from datetime import datetime

session = get_cached_session()

st.title('Price Zone Consolidation')

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

moving_zone_label = st.sidebar.selectbox(
    "Moving Zone", 
    zone_map_df['ZONENAME'], 
    index = 0,
    key = "moving_zone"
)

target_zone_label = st.sidebar.selectbox(
    "Target Zone",
    zone_map_df['ZONENAME'], 
    index = 1,
    key = "target_zone"
)

if st.sidebar.button("Compare Zone Prices"):
    st.session_state.load_data = True

moving_zone_key = price_zones_dict.get(moving_zone_label)
target_zone_key = price_zones_dict.get(target_zone_label)

@st.cache_data(show_spinner = True)
def compare_zones(eff_date, moving_zone_key, target_zone_key):
    vendor_df = session.table('edl.phq.vendor_master').select(
        col('v_id').alias('vm_v_id'),
        col('vendor'),
        col('vendor_name')
    )

    im_df = session.table('edl.phq.item_master').select(
        col('item_id').alias('im_item_id'),
        col('upc_ean')
    )

    item_df = session.table('edw.rtl.retail_item_vw').select(
        col('item_id').alias('ri_item_id'),
        col('product_upc').alias('product_upc'),
        col('item_size_uom_desc').alias('unit_size'),
        col('item_description').alias('item_description'),
        col('item_brand_desc').alias('brand'),
        col('mdse_grp_key').alias('group_id'),
        col('mdse_catgy_key').alias('category_id')
    )

    anchor_df = session.table('sbx_biz.marketing.v_anchor_detail')

    item_df = item_df.join(
        anchor_df,
        item_df['ri_item_id'] == anchor_df['upc'],
        how = 'left'
    )

    item_df = item_df.with_column(
        'UPC-Apid',
        when(col("apid").is_not_null(), concat(lit("a_"), col("apid").cast('string')))
        .otherwise(col("ri_item_id").cast('string'))
    )

    moving_reg_item_prices_df = gd.get_reg_item_prices(eff_date, moving_zone_key)
    target_reg_item_prices_df = gd.get_reg_item_prices(eff_date, target_zone_key)

    moving_promo_item_prices_df = gd.get_promo_item_prices(eff_date, moving_zone_key)
    target_promo_item_prices_df = gd.get_promo_item_prices(eff_date, target_zone_key)

    moving_mvmt_df = gd.get_26w_movement(eff_date, moving_zone_key)
    target_mvmt_df = gd.get_26w_movement(eff_date, target_zone_key)

    diff_reg_item_prices_df = moving_reg_item_prices_df.join(
        target_reg_item_prices_df,
        on = 
            (moving_reg_item_prices_df['item_id'] == target_reg_item_prices_df['item_id']) &
            (moving_reg_item_prices_df['v_id'] == target_reg_item_prices_df['v_id']),
        how = 'fullouter'
        ).filter(
            (moving_reg_item_prices_df['unit_price'].is_null()) |
            (target_reg_item_prices_df['unit_price'].is_null()) |
            (moving_reg_item_prices_df['unit_price'] != target_reg_item_prices_df['unit_price'])
        ).select(
            moving_reg_item_prices_df['item_id'].alias('moving_item_id'),
            moving_reg_item_prices_df['v_id'].alias('moving_v_id'),
            moving_reg_item_prices_df['zonename'].alias('moving_zone'),
            moving_reg_item_prices_df['price_strategy'].alias('moving_price_strategy'),
            moving_reg_item_prices_df['start_date'].alias('moving_start'),
            moving_reg_item_prices_df['end_date'].alias('moving_end'),
            moving_reg_item_prices_df['price_multiple'].alias('moving_multiple'),
            moving_reg_item_prices_df['unit_price'].alias('moving_retail'),
            target_reg_item_prices_df['item_id'].alias('target_item_id'),
            target_reg_item_prices_df['v_id'].alias('target_v_id'),
            target_reg_item_prices_df['price_strategy'].alias('target_price_strategy'),
            target_reg_item_prices_df['zonename'].alias('target_zone'),
            target_reg_item_prices_df['start_date'].alias('target_start'),
            target_reg_item_prices_df['end_date'].alias('target_end'),
            target_reg_item_prices_df['price_multiple'].alias('target_multiple'),
            target_reg_item_prices_df['unit_price'].alias('target_retail')
        )

    df = diff_reg_item_prices_df.join(
        moving_promo_item_prices_df,
        on = 
            (diff_reg_item_prices_df['moving_item_id'] == moving_promo_item_prices_df['item_id']) &
            (diff_reg_item_prices_df['moving_v_id'] == moving_promo_item_prices_df['v_id']),
        how = 'left'
    ).join(
        target_promo_item_prices_df,
        on = 
            (diff_reg_item_prices_df['target_item_id'] == target_promo_item_prices_df['item_id']) &
            (diff_reg_item_prices_df['target_v_id'] == target_promo_item_prices_df['v_id']),
        how = 'left'
    ).join(
        moving_mvmt_df,
        on =
            (diff_reg_item_prices_df['moving_price_strategy'] == moving_mvmt_df['zonecode']) &
            (diff_reg_item_prices_df['moving_item_id'] == moving_mvmt_df['sales_item_id']),
        how = 'left'
    ).join(
        target_mvmt_df,
        on =
            (diff_reg_item_prices_df['moving_price_strategy'] == target_mvmt_df['zonecode']) &
            (diff_reg_item_prices_df['target_item_id'] == target_mvmt_df['sales_item_id']),
        how = 'left'
    ).join(
        vendor_df,
        coalesce(
            diff_reg_item_prices_df['moving_v_id'],
            diff_reg_item_prices_df['target_v_id']
        ) == vendor_df['vm_v_id'],
        how = 'left'
    ).join(
        im_df,
        coalesce(
            diff_reg_item_prices_df['moving_item_id'],
            diff_reg_item_prices_df['target_item_id']
        ) == im_df['im_item_id'],
        how = 'left'
    ).join(
        item_df,
        im_df['upc_ean'] == item_df['product_upc'],
        how = 'left'
    ).select(
        item_df['group_id'].alias('Group ID'),
        item_df['category_id'].alias('Category ID'),
        item_df['UPC-Apid'].alias('UPC-Apid'),
        item_df['ri_item_id'].alias('UPC'),
        item_df['brand'].alias('"Brand"'),
        item_df['item_description'].alias('Item Description'),
        item_df['unit_size'].alias('Unit Size'),
        vendor_df['vendor'].alias('Vendor Number'),
        vendor_df['vendor_name'].alias('"Vendor"'),
        when(
            diff_reg_item_prices_df['moving_item_id'].is_null(),
            'Add to Moving'
        ).when(
            diff_reg_item_prices_df['target_item_id'].is_null(),
            'Add to Target'
        ).when(
            moving_mvmt_df['26w_mvmt'].is_null(),
            'Change Moving ASAP'
        ).when(
            moving_mvmt_df['26w_mvmt'].is_not_null(),
            'Change Moving Slow'
        ).otherwise(
            'Audit'
        ).alias('"Action"'),
        when(
            diff_reg_item_prices_df['moving_retail'] < diff_reg_item_prices_df['target_retail'],
            'up'
        ).when(
            diff_reg_item_prices_df['moving_retail'] > diff_reg_item_prices_df['target_retail'],
            'down'
        ).otherwise(
            '---'
        ).alias('Price Up/Dn'),
        when(
            diff_reg_item_prices_df['moving_retail'].is_not_null() &
            diff_reg_item_prices_df['target_retail'].is_not_null(),
            diff_reg_item_prices_df['target_retail'] - diff_reg_item_prices_df['moving_retail']
        ).alias('Price Variance'),
            when(
            diff_reg_item_prices_df['moving_retail'].is_not_null() &
            diff_reg_item_prices_df['target_retail'].is_not_null(),
            (diff_reg_item_prices_df['target_retail'] - diff_reg_item_prices_df['moving_retail']) /
            diff_reg_item_prices_df['moving_retail']
        ).alias('Price Variance %'),
        lit(moving_zone_label).alias('Moving Zone'),
        to_date(diff_reg_item_prices_df['moving_start']).alias('M From'),
        to_date(diff_reg_item_prices_df['moving_end']).alias('M Through'),
        diff_reg_item_prices_df['moving_multiple'].alias('M Multiple'),
        diff_reg_item_prices_df['moving_retail'].alias('M Retail'),
        moving_mvmt_df['26w_mvmt'].alias('M 26w Mvmt'),
        lit(target_zone_label).alias('Target Zone'),
        to_date(diff_reg_item_prices_df['target_start']).alias('T From'),
        to_date(diff_reg_item_prices_df['target_end']).alias('T Through'),
        diff_reg_item_prices_df['target_multiple'].alias('T Multiple'),
        diff_reg_item_prices_df['target_retail'].alias('T Retail'),
        target_mvmt_df['26w_mvmt'].alias('T 26w Mvmt'),
        moving_promo_item_prices_df['description'].alias('M Promo'),
        to_date(moving_promo_item_prices_df['start_date']).alias('M Promo From'),
        to_date(moving_promo_item_prices_df['end_date']).alias('M Promo Through'),
        moving_promo_item_prices_df['price_multiple'].alias('M Promo Multiple'),
        moving_promo_item_prices_df['unit_price'].alias('M Promo Retail'),
        target_promo_item_prices_df['description'].alias('T Promo'),
        to_date(target_promo_item_prices_df['start_date']).alias('T Promo From'),
        to_date(target_promo_item_prices_df['end_date']).alias('T Promo Through'),
        target_promo_item_prices_df['price_multiple'].alias('T Promo Multiple'),
        target_promo_item_prices_df['unit_price'].alias('T Promo Retail')
    )

    df = df.to_pandas()

    return df

if st.session_state.get("load_data", False):
    df = compare_zones(eff_date, moving_zone_key, target_zone_key)

    st.sidebar.divider()

    action_selection = st.sidebar.multiselect(
        "Action", 
        sorted(df['Action'].unique()),
        key = "Action"
    )

    movement_selection = st.sidebar.multiselect(
        "Price Up/Dn", 
        sorted(df['Price Up/Dn'].dropna().unique()),
        key = "Price Up/Dn"
    )

    group_selection = st.sidebar.multiselect(
        'Group ID',
        sorted(df['Group ID'].dropna().astype(int).unique()),
        key = "Group ID"
    )

    category_selection = st.sidebar.multiselect(
        'Category ID',
        sorted(df['Category ID'].dropna().astype(int).unique()),
        key = "Category ID"
    )

    vendor_selection = st.sidebar.multiselect(
        'Vendor',
        sorted(df['Vendor'].dropna().unique()),
        key = "Vendor"
    )

    moving_promo_selection = st.sidebar.multiselect(
        'Moving Promo',
        sorted(df['M Promo'].dropna().unique()),
        key = "M Promo"
    )

    target_promo_selection = st.sidebar.multiselect(
        'Target Promo',
        sorted(df['M Promo'].dropna().unique()),
        key = "T Promo"
    )

    filtered_df = df.copy()

    if action_selection:
        filtered_df = filtered_df[filtered_df['Action'].isin(action_selection)]

    if movement_selection:
        filtered_df = filtered_df[filtered_df['Price Up/Dn'].isin(movement_selection)]

    if group_selection:
        filtered_df = filtered_df[filtered_df['Group ID'].isin(group_selection)]

    if category_selection:
        filtered_df = filtered_df[filtered_df['Category ID'].isin(category_selection)]

    if vendor_selection:
        filtered_df = filtered_df[filtered_df['Vendor'].isin(vendor_selection)]

    if moving_promo_selection:
        filtered_df = filtered_df[filtered_df['M Promo'].isin(movement_selection)]

    if target_promo_selection:
        filtered_df = filtered_df[filtered_df['T Promo'].isin(target_promo_selection)]


    st.success(f"Comparing Moving Zone {moving_zone_label} to Target Zone {target_zone_label} for prices as of {eff_date.strftime('%A, %B %d, %Y')}.")
    st.dataframe(filtered_df, hide_index = True)

    filtered_df["M From"] = pd.to_datetime(filtered_df["M From"], errors="coerce")
    filtered_df["T From"] = pd.to_datetime(filtered_df["T From"], errors="coerce")
    filtered_df["Moving Price Age"] = (datetime.now() - filtered_df["M From"]).dt.days
    filtered_df["Target Price Age"] = (datetime.now() - filtered_df["T From"]).dt.days
    m_age_days = filtered_df["Moving Price Age"].mean()
    t_age_days = filtered_df['Target Price Age'].mean()

    action_count_df = df['Action'].value_counts()
    action_count_df = action_count_df.rename("Count")

    movement_count_df = df['Price Up/Dn'].value_counts()
    movement_count_df = movement_count_df.rename("Count")


    col1, col2, col3, col4 = st.columns(4)

    col1.metric(label = "Total Items", value = len(filtered_df))
    
    if pd.notna(m_age_days):
        col2.metric(label = "Avg Moving Zone Price Age (Days)", value = int(round(m_age_days, 0)))
    else:
        col2.metric(label = "Avg Moving Zone Price Age (Days)", value = "N/A")
    
    if pd.notna(t_age_days):
        col2.metric(label = "Avg Target Zone Price Age (Days)", value = int(round(t_age_days, 0)))
    else:
        col2.metric(label = "Avg Target Zone Price Age (Days)", value = "N/A")

    col3.dataframe(action_count_df)
    col4.dataframe(movement_count_df)

    percentages = filtered_df['Price Variance %'].dropna() * 100

    percentages = percentages.clip(-100, 100)

    sn_hex = '#006a52'

    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(15, 4.5))
    sns.histplot(percentages, bins=40, kde=True, color=sn_hex)
    plt.title('Distribution of Price Variance %')
    plt.xlabel('Percentage Change')
    plt.xlim(-100, 100)
    plt.tight_layout()
    plt.show()

    st.pyplot(plt)


else:
    st.info("Click 'Compare Zone Prices' to generate data.")





