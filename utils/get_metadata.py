def get_databases(session):
    df = session.sql("SHOW DATABASES").to_pandas()
    return df['"name"']

def get_schemas(session, database):
    df = session.sql(f"SHOW SCHEMAS IN DATABASE {database}").to_pandas()
    return df['"name"']

def get_tables(session, database, schema):
    df = session.sql(f"SHOW TABLES IN {database}.{schema}").to_pandas()
    #return df[df['name'].str.startswith(fact_table_prefix)]['name'].tolist()
    return df['"name"']

def get_table_columns(session, database, schema, table):
    df = session.sql(f"DESC TABLE {database}.{schema}.{table}").to_pandas()
    return df['"name"']