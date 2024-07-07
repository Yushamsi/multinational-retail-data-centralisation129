import yaml
import database_utils
import pandas as pd
import tabula

table_name = 'legacy_users'

class DataExtractor:
    def __init__(self, yaml_file):
        self.yaml_file = yaml_file
    
    def read_rds_table(self, table_name):
        DatabaseConnector = database_utils.DatabaseConnector(self.yaml_file)
        tables_list = DatabaseConnector.list_db_tables()
        user_table_pd = pd.read_sql_table(table_name, DatabaseConnector.init_db_engine()) #hard-coded
        return user_table_pd
    
    def retrieve_pdf_data(self, link):
        extracted_pdf_pd = tabula.read_pdf(link)
        return extracted_pdf_pd
        
        
        
        
       
    
    
        
    
         



