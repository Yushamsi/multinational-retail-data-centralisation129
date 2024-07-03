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
    
    def init_db_engine(self):
        credentials = read_db_creds(self.yaml_file)
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = credentials.RDS_HOST
        USER = credentials.RDS_USER
        PASSWORD = credentials.RDS_PASSWORD
        DATABASE = credentials.RDS_DATABASE
        PORT = credentials.RDS_PORT
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        # engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        # engine.connect()
        return engine
        
    def list_db_tables():
        inspector = inspect(init_db_engine())
        tables = inspector.get_table_names()
        print(tables)
        
