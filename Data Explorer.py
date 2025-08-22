from utils.session import get_cached_session
import streamlit as st

session = get_cached_session()

table_name = st.text_input("Enter a table name:", "MY_TABLE")

if table_name:
    try:
        df = session.table(table_name).limit(10).to_pandas()
        st.dataframe(df)
    except Exception as e:
        st.error(f"Could not load table {table_name}: {e}")
