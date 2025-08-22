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

    store_zone_df = session.table('EDL.PHQ.SN_REV_ZONEGRP').filter(
        col('ZONECODE') == zone_key
    ).select(
        col('ZONECODE'),
        col('STORECODE')
    ).distinct()

    item_df = session.table('EDW.RTL.RETAIL_ITEM_VW').select(
        col('ITEM_DESCRIPTION'),
        col('PRODUCT_UPC')
    )

    pt_type_df = session.table('EDL.PHQ.PRICE_TYPE').select(
        col('PT_TYPE'),
        col('DESCRIPTION').alias('PT_DESCRIPTION')
    )

    store_item_prices_df = session.table('EDL.PHQ.ITEM_RETAIL_PRICE_DT').filter(
        (eff_date >= to_date(col('IP_START_DATE'))) & 
        (eff_date <= coalesce(to_date(col('IP_END_DATE')), lit('9999-12-31')))
    )

    store_item_prices_df = store_item_prices_df.join(
        item_df,
        on = item_df['PRODUCT_UPC'] == col('FULL_UPC_NBR'),
        how = 'inner'
    ).join(
        store_zone_df,
        on = store_item_prices_df['STORE_ID'] == store_zone_df['STORECODE'],
        how = 'inner'
    ).join(
        pt_type_df,
        on = store_item_prices_df['PT_TYPE'] == pt_type_df['PT_TYPE'],
        how = 'inner'
    ).select(
        col('ZONECODE'),
        col('FULL_UPC_NBR'),
        col('ITEM_DESCRIPTION'),
        col('IP_UNIT_PRICE'),
        col('IP_PRICE_MULTIPLE'),
        col('IP_START_DATE'),
        col('IP_END_DATE'),
        col('PT_DESCRIPTION')
    )

    zone_item_prices_df = (
        store_item_prices_df.group_by(
            col('ZONECODE'),
            col('FULL_UPC_NBR'),
        )
        .agg(
            # Regular metrics
            mode(when(col('PT_DESCRIPTION') == 'Regular', col('IP_UNIT_PRICE'))).alias('IP_UNIT_PRICE'),
            mode(when(col('PT_DESCRIPTION') == 'Regular', col('IP_PRICE_MULTIPLE'))).alias('IP_PRICE_MULTIPLE'),
            mode(when(col('PT_DESCRIPTION') == 'Regular', col('IP_START_DATE'))).alias('IP_START_DATE'),
            mode(when(col('PT_DESCRIPTION') == 'Regular', col('IP_END_DATE'))).alias('IP_END_DATE'),
            count(when(col('PT_DESCRIPTION') == 'Regular', 1)).alias('STORE_COUNT'),

            # Promo metrics
            mode(when(col('PT_DESCRIPTION') != 'Regular', col('PT_DESCRIPTION'))).alias('PROMO_TYPE'),
            mode(when(col('PT_DESCRIPTION') != 'Regular', col('IP_UNIT_PRICE'))).alias('PROMO_UNIT_PRICE'),
            mode(when(col('PT_DESCRIPTION') != 'Regular', col('IP_PRICE_MULTIPLE'))).alias('PROMO_PRICE_MULTIPLE'),
            mode(when(col('PT_DESCRIPTION') != 'Regular', col('IP_START_DATE'))).alias('PROMO_START_DATE'),
            mode(when(col('PT_DESCRIPTION') != 'Regular', col('IP_END_DATE'))).alias('PROMO_END_DATE'),
            count(when(col('PT_DESCRIPTION') != 'Regular', 1)).alias('PROMO_STORE_COUNT')
        )
        .join(
            item_df,
            item_df['PRODUCT_UPC'] == col('FULL_UPC_NBR'),
            how='left'
        )
        .select(
            col('ZONECODE'),
            col('FULL_UPC_NBR'),
            col('ITEM_DESCRIPTION'),
            col('IP_UNIT_PRICE'),
            col('IP_PRICE_MULTIPLE'),
            col('IP_START_DATE'),
            col('IP_END_DATE'),
            col('STORE_COUNT'),
            col('PROMO_TYPE'),
            col('PROMO_UNIT_PRICE'),
            col('PROMO_PRICE_MULTIPLE'),
            col('PROMO_START_DATE'),
            col('PROMO_END_DATE'),
            col('PROMO_STORE_COUNT')
        )
    )

    return zone_item_prices_df


