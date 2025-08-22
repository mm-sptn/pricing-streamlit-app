from utils.session import get_cached_session
import utils.get_data as gd
import streamlit as st

session = get_cached_session()

temp_df = session.table('EDW.RTL.RETAIL_SALES').limit(10).to_pandas()

df = gd.get_price_strategies()

st.write(df)