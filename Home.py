import streamlit as st
import utils.get_data as gd

st.title("Welcome to the Pricing App")
zone_map_df = gd.get_price_strategies()
