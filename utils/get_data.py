import streamlit as st
from snowflake.snowpark.functions import col, to_date, coalesce, lit, mode, count, when, row_number, to_date
from snowflake.snowpark.window import Window
from datetime import datetime, timedelta
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

def get_reg_item_prices(eff_date, zone_key):
    session = get_cached_session()

    ip_base = (
        session.table('edl.phq.item_price')
        .filter(
            (col('ip_start_date') <= eff_date) &
            (coalesce(to_date(col('IP_END_DATE')), lit('9999-12-31')) >= eff_date) &
            (col('record_status') != 3) &
            (col('pt_type') == 1) &
            (col('price_strategy') == zone_key)
        )
    )

    w = Window.partition_by(col('item_id'), col('v_id'), col('pt_type')).order_by(col('ip_start_date').desc())

    ip_df = (
        ip_base
        .with_column("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select(
            col('item_id').alias('item_id'),
            col('v_id').alias('v_id'),
            col('ip_unit_price').alias('unit_price'),
            col('ip_price_multiple').alias('price_multiple'),
            col('ip_start_date').alias('start_date'),
            col('ip_end_date').alias('end_date'),
            col('price_strategy').alias('price_strategy')
        )
    )

    zg_df = session.table('edl.phq.sn_rev_zonegrp').filter(
        col('zonegroupcode') == 1
    ).select(
        col('zonecode'),
        col('zonename')
    ).distinct()

    df = ip_df.join(
        zg_df,
        ip_df['price_strategy'] == zg_df['zonecode'],
        how = 'inner'
    )

    return df

def get_promo_item_prices(eff_date, zone_key):
    session = get_cached_session()

    ad_groups_df = session.table('edl.phq.ad_group_stores').filter(
        (col('record_status') != 3)
    )

    zones_df = session.table('edl.phq.sn_rev_zonegrp').filter(
        (col('zonegroupcode') == 1) &
        (col('zonecode') == zone_key)
    )

    pt_df = session.table('edl.phq.price_type').select(
        col('pt_type').alias('pt_pt_type'),
        col('description')
    )

    ag_zone_mapping = ad_groups_df.join(
        zones_df,
        ad_groups_df['store_id'] == zones_df['storecode'],
        how = 'inner'
    ).select(
        col('ad_group'),
        col('zonecode'),
        col('zonename')
    ).distinct()

    ip_base = (
        session.table('edl.phq.item_price')
        .filter(
            (col('ip_start_date') <= eff_date) &
            (coalesce(to_date(col('IP_END_DATE')), lit('9999-12-31')) >= eff_date) &
            (col('record_status') != 3) &
            (col('pt_type') != 1) &
            (col('store_id') == 0)
        )
    )

    w = Window.partition_by(col('item_id'), col('v_id'), col('pt_type')).order_by(col('ip_start_date').desc())

    ip_df = (
        ip_base
        .with_column("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select(
            col('item_id').alias('item_id'),
            col('v_id').alias('v_id'),
            col('ip_unit_price').alias('unit_price'),
            col('ip_price_multiple').alias('price_multiple'),
            col('ip_start_date').alias('start_date'),
            col('ip_end_date').alias('end_date'),
            col('pt_type'),
            col('ad_group')
        )
    )

    df = ip_df.join(
        ag_zone_mapping,
        ip_df['ad_group'] == ag_zone_mapping['ad_group'],
        how = 'inner'
    ).join(
        pt_df,
        ip_df['pt_type'] == pt_df['pt_pt_type'],
        how = 'inner'
    )

    return df