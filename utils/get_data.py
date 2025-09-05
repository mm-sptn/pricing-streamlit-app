import streamlit as st
from snowflake.snowpark.functions import col, to_date, coalesce, lit, mode, count, when, row_number, date_add, date_trunc, dayofweek, sum as _sum

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

def get_26w_movement(eff_date, zone_key):
    session = get_cached_session()


    adjusted_end_date = date_add(
        lit(eff_date),
        -((dayofweek(lit(eff_date)) % 7)) 
    )

    start_date = date_add(adjusted_end_date, -7 * 26)

    item_df = session.table('sbx_biz.marketing.t_item').select(
        col('"RETAIL_ITEM_ID"').alias('retail_item_pk'),
        col('"Product UPC"').alias('upc')
    )

    im_df = session.table('edl.phq.item_master').select(
        col('item_id').alias('sales_item_id'),
        col('upc_ean')
    )

    item_df = item_df.join(
        im_df,
        item_df['upc'] == im_df['upc_ean'],
        how = 'inner'
    )

    zones_df = session.table('edl.phq.sn_rev_zonegrp').filter(
        (col('zonegroupcode') == 1) &
        (col('zonecode') == zone_key)
    ).select(
        col('zonecode'),
        col('storecode')
    )


    sales_df = session.table('edw.rtl.retail_sales').select(
        col('sales_date_id').alias('sales_date_id'),
        col('retail_item_id').alias('retail_item_id'),
        col('store_nbr').alias('store_nbr'),
        col('total_sales_qty').alias('total_sales_qty')
    ).with_column(
        'sales_date',
        to_date(col('sales_date_id').cast('string'), 'YYYYMMDD')
    ).filter(
        (col('sales_date') > start_date) &
        (col('sales_date') <= adjusted_end_date)
    )

    sales_df = sales_df.join(
        item_df,
        sales_df['retail_item_id'] == item_df['retail_item_pk'],
        how = 'inner'
    ).join(
        zones_df,
        sales_df['store_nbr'] == zones_df['storecode'],
        how = 'inner'
    )

    df = sales_df.group_by(
        col('zonecode'),
        col('sales_item_id')
    ).agg(
        _sum(col('total_sales_qty')).alias('26w_mvmt')
    )

    return df
    

    
