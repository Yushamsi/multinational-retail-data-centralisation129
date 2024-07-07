import data_extraction
import database_utils
import yaml
import database_utils
import pandas as pd


class DataCleaning:
    def __init__(self, yaml_file) -> None:
        self.yaml_file = yaml_file
        
    def clean_user_data(self, table_name, upload_table_name):
        extract_data = data_extraction.DataExtractor(self.yaml_file)
        table_to_clean = extract_data.read_rds_table(table_name)
        print(table_to_clean)
        
        print('Info: \n', table_to_clean.info())
        
        print('NULL Values?: \n', table_to_clean.isna().any())
        
        # print('')
        table_to_clean = table_to_clean.fillna(0)
        # table_to_clean.dropna(axis=0)
        
        connector = database_utils.DatabaseConnector
        connector.upload_to_sales_db(pandas_dataframe=table_to_clean, upload_table_name=upload_table_name)
        
    def clean_card_data(self, link, upload_table_name):
        extract_data = data_extraction.DataExtractor(self.yaml_file)
        table_to_clean = extract_data.retrieve_pdf_data(link)
        
        connector = database_utils.DatabaseConnector
        connector.upload_to_sales_db(pandas_dataframe=table_to_clean, upload_table_name=upload_table_name)
        
        
        
if __name__ == '__main__':
    table_name = 'legacy_users'
    link_to_pdf = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

    test = DataCleaning(yaml_file='db_creds.yaml')
    test.clean_user_data(table_name = 'legacy_users', upload_table_name='dim_users')
    
    test2 = DataCleaning(yaml_file='db_creds.yaml')
    test2.clean_card_data(link = link_to_pdf, upload_table_name='dim_card_details')
        