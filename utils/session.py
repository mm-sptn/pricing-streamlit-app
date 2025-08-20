import os
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from dotenv import load_dotenv

load_dotenv()

def get_session():
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
        }).create()