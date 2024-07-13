import yaml
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

class DatabaseConnector:
    def __init__(self):
        pass
    
    def read_db_creds(self, yaml_file):
        with open(yaml_file, 'r') as stream:
            data_loaded = yaml.safe_load(stream)
            return data_loaded
    
    def init_db_engine(self, yaml_file):
        credentials = self.read_db_creds(yaml_file)
        print(credentials)
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = credentials['HOST']
        USER = credentials['USER']
        PASSWORD = credentials['PASSWORD']
        DATABASE = credentials['DATABASE']
        PORT = credentials['PORT']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine = engine.connect()
        return engine
        
    def list_db_tables(self, engine):
        inspector = inspect(engine)
        tables_list = inspector.get_table_names()
        print('\nTables List: ', tables_list , '\n')
        return tables_list
    
    def upload_to_db(self, pandas_dataframe, upload_table_name, engine): 
        pandas_dataframe.to_sql(upload_table_name, engine, if_exists='replace')
        print(f'Uploaded dataframe as {upload_table_name}')
    
