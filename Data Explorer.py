from utils.session import get_cached_session
import utils.get_metadata as gm
import streamlit as st


session = get_cached_session()

databases = gm.get_databases(session)

col1, col2, col3 = st.columns(3)

with col1:
    selected_database = st.selectbox("Database", databases)
    st.markdown(
    "<p style='font-size:12px; color:orange;'>⚠️Please note: By default only 100 rows are displayed in the below preview. Alter the rows parameter to change the limit.</p>",
    unsafe_allow_html=True
    )
with col2:
    if selected_database:
        selected_schema = st.selectbox("Schema", gm.get_schemas(session, selected_database))
    else:
        st.write("Select a database first")


with col3:
    if selected_schema:
        selected_table = st.selectbox("Table", gm.get_tables(session, selected_database, selected_schema))
    else:
        st.write("Select a schema first")
    rows_limit = st.number_input("Rows", min_value = 1, value = 100)

df = session.table(f"{selected_database}.{selected_schema}.{selected_table}").limit(rows_limit).to_pandas()

st.write(df)