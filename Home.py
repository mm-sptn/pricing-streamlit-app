import streamlit as st
import utils.get_data as gd

st.title("Welcome to the Pricing App")

zone_map_df = gd.get_price_strategies()

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("#### 📘 [Merch Analytics SharePoint](https://sptn.sharepoint.com/sites/MerchAnalytics)")
    st.caption("Documentation for all things Merchandising")

with col2:
    st.markdown("#### 📊 [Power BI Merch HQ](https://app.powerbi.com/groups/me/apps/434d0660-5ac7-4f21-a393-40786c001666?experience=power-bi)")
    st.caption("Merchandising Analytics Reporting")

with col3:
    st.markdown("#### 🧑‍💻 [Develop with Streamlit](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)")
    st.caption("Create seamless applications right on top of Snowflake data for complex, digestible reporting")