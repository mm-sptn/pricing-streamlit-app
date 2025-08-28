import streamlit as st
from snowflake.snowpark.functions import col, to_date, coalesce, lit, mode, count, when
from utils.session import get_cached_session


@st.cache_data(show_spinner = "Fetching pricing strategies...")
def get_price_strategies():
    session = get_cached_session()

    zone_map_df = session.table('EDL.PHQ.SN_REV_ZONEGRP').filter(
        ((col('ZONECODE').isin([1,2,3])) |
        ((col('ZONECODE') >= 11) & (col('ZONECODE') <= 33))) & 
        (col('ZONEGROUPCODE') == 1)
    ).select(
        col('ZONENAME'),
        col('ZONECODE')
    ).distinct().order_by(col('ZONECODE'))

    zone_map_df = zone_map_df.to_pandas()
    return zone_map_df

def get_item_prices(eff_date, zone_key):
    session = get_cached_session()

    ip_df = session.table('edl.phq.item_price').filter(
        (col('ip_start_date') <= eff_date) &
        (coalesce(to_date(col('IP_END_DATE')), lit('9999-12-31')) >= eff_date) &
        (col('record_status') != 3)
    ).select(
        col('item_id').alias('ip_item_id'),
        col('item_price_id').alias('item_price_id'),
        col('v_id').alias('v_id'),
        col('ip_unit_price').alias('ip_unit_price'),
        col('ip_price_multiple').alias('ip_price_multiple'),
        col('ip_start_date').alias('ip_start_date'),
        col('ip_end_date').alias('ip_end_date'),
        col('store_id').alias('ip_store_nbr'),
        col('pt_type').alias('ip_pt_type')
    )

    im_df = session.table('edl.phq.item_master').select(
        col('item_id').alias('im_item_id'),
        col('upc_ean').alias('upc_ean')
    )

    item_df = session.table('edw.rtl.retail_item_vw').select(
        col('product_upc').alias('product_upc'),
        col('mdse_grp_key').alias('mdse_grp_key'),
        col('mdse_catgy_key').alias('mdse_catgy_key'),
        col('item_description').alias('item_description')
    )

    zg_df = session.table('edl.phq.sn_rev_zonegrp').filter(
        (col('zonegroupcode') == 1) &
        (col('zonecode') == zone_key)
    ).select(
        col('zonecode').alias('zonecode'),
        col('zonename').alias('zonename'),
        col('storecode').alias('zg_store_nbr')
    )

    pt_df = session.table('edl.phq.price_type').select(
        col('pt_type').alias('pt_pt_type'),
        col('description').alias('pt_description')
    )

    df = ip_df.join(
        zg_df,
        zg_df['zg_store_nbr'] == ip_df['ip_store_nbr'],
        how = 'inner'
    ).join(
        pt_df,
        ip_df['ip_pt_type'] == pt_df['pt_pt_type'],
        how = 'inner'
    )

    df = df.group_by(
        col('ip_item_id'),
        col('v_id'),
        col('zonecode')
        ).agg(
        mode(when(col('ip_pt_type') == 1, col('ip_unit_price'))).alias('unit_price'),
        mode(when(col('ip_pt_type') == 1, col('ip_price_multiple'))).alias('price_multiple'),
        mode(when(col('ip_pt_type') == 1, col('ip_start_date'))).alias('start_date'),
        mode(when(col('ip_pt_type') == 1, col('ip_end_date'))).alias('end_date'),
        count(when(col('ip_pt_type') == 1, lit(1))).alias('store_count'),

        mode(when(col('ip_pt_type') != 1, col('pt_description'))).alias('promo_type'),
        mode(when(col('ip_pt_type') != 1, col('ip_unit_price'))).alias('promo_unit_price'),
        mode(when(col('ip_pt_type') != 1, col('ip_price_multiple'))).alias('promo_price_multiple'),
        mode(when(col('ip_pt_type') != 1, col('ip_start_date'))).alias('promo_start_date'),
        mode(when(col('ip_pt_type') != 1, col('ip_end_date'))).alias('promo_end_date'),
        count(when(col('ip_pt_type') != 1, lit(1))).alias('promo_store_count')
        )

    return df


