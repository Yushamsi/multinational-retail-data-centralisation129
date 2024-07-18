import os

from botocore.exceptions import NoCredentialsError, ClientError
import boto3
import pandas as pd
import requests
import tabula
import yaml

import database_utils

# class DataExtractor:
#     def __init__(self):
#         self.retrieve_a_store_base = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/' 
#         self.return_store_number = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
#         self.api_header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    
#     def read_rds_table(self, table_name, engine):
#         table_pd = pd.read_sql_table(table_name, engine)
#         return table_pd
    
#     def retrieve_pdf_data(self, link, pages='all'):
#         extracted_pdf_pd = tabula.read_pdf(link, pages=pages)
#         df = pd.concat(extracted_pdf_pd, ignore_index=True)
#         return df
    
#     def list_number_of_stores(self):
#         # Send a GET request to the API
#         response = requests.get(self.return_store_number, headers=self.api_header)

#         # Check if the request was successful (status code 200)
#         if response.status_code == 200:
#             # Access the response data as JSON
#             data = response.json()
#             return data['number_stores']
            
#         # If the request was not successful, print the status code and response text
#         else:
#             print(f"Request failed with status code: {response.status_code}")
#             print(f"Response Text: {response.text}")
            
#     def retrieve_stores_data(self):
#         number_of_stores = self.list_number_of_stores()
#         print(number_of_stores)
#         all_stores_data = []

#         for store_number in range(0, number_of_stores):
#             store_url = f"{self.retrieve_a_store_base}{store_number}"
#             print(store_number)
#             print(store_url)
#             try:
#                 response = requests.get(store_url, headers=self.api_header)
#                 response.raise_for_status()
#                 store_data = response.json()
#                 data_dict = pd.DataFrame(store_data, index=[store_number])
#                 all_stores_data.append(data_dict)
                
#             except requests.RequestException as e:
#                 print(f"Error retrieving data for store {store_number}: {e}")

#         # Concatenate all DataFrame objects into a single DataFrame
#         if all_stores_data:
#             print(all_stores_data)
#             store_dataframe = pd.concat(all_stores_data, ignore_index=True)
#             return store_dataframe
#         else:
#             return pd.DataFrame()
        
#     def extract_from_s3(self, s3_address):
#         if not isinstance(s3_address, str):
#             raise ValueError("s3_address must be a string containing the S3 URI or an HTTP URL")

#         try:
#             s3 = boto3.client('s3')
#             # Check the protocol of the URI
#             if s3_address.startswith('https://'):
#                 url_parts = s3_address.replace("https://", "").split('/')
#                 url_split = url_parts[0].split('.')
#                 bucket = url_split[0]
#                 key = '/'.join(url_parts[1:])
#                 file = f'tables & csv/{key}'
#                 s3.download_file(Bucket=bucket, Key=key, Filename=f'tables & csv/{key}')
#                 print("*** Data Downloaded via S3 ***")
                
#             elif s3_address.startswith('s3://'):
#                 # Handle S3 URI using the format you prefer
#                 uri_split = s3_address.split('/')
#                 bucket = uri_split[2]
#                 key = '/'.join(uri_split[3:])
#                 s3.download_file(Bucket=bucket, Key=key, Filename=f"tables & csv/{key}")
#                 print("*** Data Downloaded via S3 ***")
                
#             else:
#                 raise ValueError("URL must start with either https:// or s3://")
            
#             # Load data into DataFrame based on file extension
#             if key.endswith('.csv'):
#                 dataframe = pd.read_csv(f"tables & csv/{key}")
#                 print("--- DataFrame Extracted ---")
#                 return dataframe
#             elif key.endswith('.json'):
#                 dataframe = pd.read_json(f"tables & csv/{key}")
#                 print("--- DataFrame Extracted ---")
#                 return dataframe

#         except NoCredentialsError:
#             print("AWS credentials not found. Please configure your credentials.")

#         except ClientError as e:
#             if e.response['Error']['Code'] == 'NoSuchBucket':
#                 print("The specified bucket does not exist.")
#             else:
#                 print("An error occurred:", e)

class DataExtractor:
    """
    A class to handle data extraction from various sources including RDS, PDFs, APIs, and AWS S3.
    """
    
    def __init__(self):
        """
        Initializes the DataExtractor instance with API endpoints and headers.
        """
        self.retrieve_a_store_base = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/' 
        self.return_store_number = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
        self.api_header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    
    def read_rds_table(self, table_name: str, engine: object) -> pd.DataFrame:
        """
        Reads a table from an RDS instance into a pandas DataFrame.

        Parameters:
            table_name (str): The name of the table to read.
            engine (Engine): The SQLAlchemy Engine instance connected to the RDS.

        Returns:
            pd.DataFrame: A DataFrame containing the data read from the table.
        """
        table_pd = pd.read_sql_table(table_name, engine)
        return table_pd
    
    def retrieve_pdf_data(self, link: str, pages: str = 'all') -> pd.DataFrame:
        """
        Extracts data from a PDF file available at a specified link.

        Parameters:
            link (str): The URL to the PDF file.
            pages (str): Specific pages to extract ('all' or pages numbers like '1,2,3').

        Returns:
            pd.DataFrame: A DataFrame containing the data extracted from the PDF.
        """
        extracted_pdf_pd = tabula.read_pdf(link, pages=pages)
        df = pd.concat(extracted_pdf_pd, ignore_index=True)
        return df
    
    def list_number_of_stores(self) -> int:
        """
        Retrieves the number of stores from an API.

        Returns:
            int: The number of stores returned by the API.
        """
        response = requests.get(self.return_store_number, headers=self.api_header)
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            data = response.json()
            return data['number_stores']
        
        # If the request was not successful, print the status code and response text
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")
            return 0
    
    def retrieve_stores_data(self) -> pd.DataFrame:
        """
        Retrieves and aggregates data for multiple stores.

        Returns:
            pd.DataFrame: A DataFrame containing the aggregated data of all stores.
        """
        number_of_stores = self.list_number_of_stores()
        print(number_of_stores)
        all_stores_data = []
        for store_number in range(0, number_of_stores):
            store_url = f"{self.retrieve_a_store_base}{store_number}"
            print(store_number)
            print(store_url)
            try:
                response = requests.get(store_url, headers=self.api_header)
                response.raise_for_status()
                store_data = response.json()
                data_dict = pd.DataFrame(store_data, index=[store_number])
                all_stores_data.append(data_dict)
            except requests.RequestException as e:
                print(f"Error retrieving data for store {store_number}: {e}")

        # Concatenate all DataFrame objects into a single DataFrame
        if all_stores_data:
            print(all_stores_data)
            store_dataframe = pd.concat(all_stores_data, ignore_index=True)
            return store_dataframe
        else:
            return pd.DataFrame()
        
    def extract_from_s3(self, s3_address: str) -> pd.DataFrame:
        """
        Extracts data from an AWS S3 bucket based on the specified S3 address.

        Parameters:
            s3_address (str): The URI or HTTP URL to the S3 bucket containing the data.

        Returns:
            pd.DataFrame: A DataFrame containing the data extracted from the S3 bucket.
        """
        if not isinstance(s3_address, str):
            raise ValueError("s3_address must be a string containing the S3 URI or an HTTP URL")

        try:
            s3 = boto3.client('s3')
            if s3_address.startswith('https://'):
                url_parts = s3_address.replace("https://", "").split('/')
                bucket = url_parts[0].split('.')[0]
                key = '/'.join(url_parts[1:])
                s3.download_file(Bucket=bucket, Key=key, Filename=f'tables & csv/{key}')
                print("*** Data Downloaded via S3 ***")
            elif s3_address.startswith('s3://'):
                uri_split = s3_address.split('/')
                bucket = uri_split[2]
                key = '/'.join(uri_split[3:])
                s3.download_file(Bucket=bucket, Key=key, Filename=f"tables & csv/{key}")
                print("*** Data Downloaded via S3 ***")
            else:
                raise ValueError("URL must start with either https:// or s3://")
            # Load data into DataFrame based on file extension
            if key.endswith('.csv'):
                dataframe = pd.read_csv(f"tables & csv/{key}")
                print("--- DataFrame Extracted ---")
                return dataframe
            elif key.endswith('.json'):
                dataframe = pd.read_json(f"tables & csv/{key}")
                print("--- DataFrame Extracted ---")
                return dataframe
            
        except NoCredentialsError:
            print("AWS credentials not found. Please configure your credentials.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print("The specified bucket does not exist.")
            else:
                print("An error occurred:", e)
            

# if __name__ == '__main__':
#     test = DataExtractor()
#     df = test.extract_from_s3("s3://data-handling-public/products.csv")
#     print(df)
        
        
       
    
    
        
    
         



