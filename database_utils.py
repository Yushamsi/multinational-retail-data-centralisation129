from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import psycopg2
import yaml

class DatabaseConnector:
    """
    A class to handle database connections and operations.
    """
    
    def __init__(self):
        """
        Initializes the DatabaseConnector instance.
        """
        pass

    def read_db_creds(self, yaml_file: str) -> dict:
        """
        Reads database credentials from a YAML file.

        Parameters:
            yaml_file (str): The path to the YAML file containing the database credentials.

        Returns:
            dict: A dictionary containing the database credentials.
        """
        with open(yaml_file, 'r') as stream:
            data_loaded = yaml.safe_load(stream)
            return data_loaded
    
    def init_db_engine(self, yaml_file: str):
        """
        Initializes the database engine using credentials from a specified YAML file.

        Parameters:
            yaml_file (str): The path to the YAML file containing the database credentials.

        Returns:
            Engine: A SQLAlchemy Engine instance connected to the specified database.
        """
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
        
    def list_db_tables(self, engine) -> list:
        """
        Lists all the tables in the connected database.

        Parameters:
            engine (Engine): The SQLAlchemy Engine instance connected to the database.

        Returns:
            list: A list of table names in the database.
        """
        inspector = inspect(engine)
        tables_list = inspector.get_table_names()
        print('\nTables List: ', tables_list, '\n')
        return tables_list
    
    def upload_to_db(self, pandas_dataframe, upload_table_name: str, engine) -> None:
        """
        Uploads a pandas DataFrame to a specified table in the database.

        Parameters:
            pandas_dataframe (DataFrame): The pandas DataFrame to be uploaded.
            upload_table_name (str): The name of the target table in the database.
            engine (Engine): The SQLAlchemy Engine instance connected to the database.

        Returns:
            None
        """
        pandas_dataframe.to_sql(upload_table_name, engine, if_exists='replace')
        print(f'Uploaded dataframe as {upload_table_name}')