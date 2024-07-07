import yaml
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

class DatabaseConnector:
    def __init__(self, yaml_file):
        self.yaml_file = yaml_file
        
    def read_db_creds(self):
        with open(self.yaml_file, 'r') as stream:
            data_loaded = yaml.safe_load(stream)
            return data_loaded
    
    def init_db_engine(self): #hard-coded
        credentials = self.read_db_creds()
        print(credentials)
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = credentials['RDS_HOST']
        USER = credentials['RDS_USER']
        PASSWORD = credentials['RDS_PASSWORD']
        DATABASE = credentials['RDS_DATABASE']
        PORT = credentials['RDS_PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        # engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        # engine.connect()
        return engine
        
    def list_db_tables(self):
        inspector = inspect(self.init_db_engine())
        tables_list = inspector.get_table_names()
        print('\n Tables List: ', tables_list , '\n')
        return tables_list
    
    def upload_to_sales_db(pandas_dataframe, upload_table_name): #hard-coded
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'postgres'
        DATABASE = 'sales_data'
        PORT = 5432
        local_engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        pandas_dataframe.to_sql(upload_table_name, local_engine, if_exists='replace')
        
if __name__ == '__main__':
    find_list_tables = DatabaseConnector("db_creds.yaml")
    find_list_tables.list_db_tables()
