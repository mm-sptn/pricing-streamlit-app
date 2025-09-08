from utils.session import get_cached_session
import utils.get_data as gd
import streamlit as st

session = get_cached_session()

df = gd.get_price_strategies()

# Define pages
home = st.Page("Home.py", title="🏠 Home")
data_explorer = st.Page("Data Explorer.py", title="📊 Data Explorer")
pz_consolidation = st.Page("Price Zone Consolidation.py", title="💲 Price Zone Consolidation")

# Navigation
pg = st.navigation([home, data_explorer, pz_consolidation])

# App config
st.set_page_config(page_title="Pricing App", page_icon=":moneybag:", layout="wide")


# Run nav
pg.run()