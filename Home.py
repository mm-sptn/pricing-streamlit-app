from utils.session import get_session

session = get_session()

temp_df = session.table('EDW.RTL.RETAIL_SALES').limit(10).to_pandas()