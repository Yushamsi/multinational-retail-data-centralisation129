import data_cleaning, data_extraction, database_utils
import yaml
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

rds = database_utils.DatabaseConnector()
rds_engine = rds.init_db_engine('db_creds.yaml')

list_tables = rds.list_db_tables(rds_engine)
print(list_tables)

# table_name = 'legacy_users'

# test = database_utils.DatabaseConnector()
# test_engine = test.init_db_engine('my_sales_db.yaml')

# rds = database_utils.DatabaseConnector()
# rds_engine = test.init_db_engine('db_creds.yaml')

# extract = data_extraction.DataExtractor()

# data = extract.read_rds_table(table_name = 'legacy_users', engine=rds_engine)

# test.upload_to_db(pandas_dataframe=data, upload_table_name='dim_users', engine=test_engine)