import os
import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session


def get_session():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ModuleNotFoundError:
        pass
    try:
        return get_active_session()
    except Exception:
        return Session.builder.configs({
            "account": os.getenv("SF_ACCOUNT"),
            "user": os.getenv("SF_USER"),
            "authenticator": os.getenv("SF_AUTH"),
            "role": os.getenv("SF_ROLE"),
            "database": os.getenv("SF_DATABASE"),
            "schema": os.getenv("SF_SCHEMA"),
            "warehouse": os.getenv("SF_WAREHOUSE")
        }).create()

@st.cache_resource(show_spinner = "Connecting to Snowflake...")
def get_cached_session():
    return get_session()