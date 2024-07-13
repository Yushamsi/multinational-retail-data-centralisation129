import yaml
import database_utils
import pandas as pd
import tabula
import requests
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

table_name = 'legacy_users'

class DataExtractor:
    def __init__(self):
        self.retrieve_a_store_base = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/' 
        self.return_store_number = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
        self.api_header = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    
    def read_rds_table(self, table_name, engine):
        table_pd = pd.read_sql_table(table_name, engine)
        return table_pd
    
    def retrieve_pdf_data(self, link, pages='all'):
        extracted_pdf_pd = tabula.read_pdf(link, pages=pages)
        df = pd.concat(extracted_pdf_pd, ignore_index=True)
        return df
    
    def list_number_of_stores(self):
        # Send a GET request to the API
        response = requests.get(self.return_store_number, headers=self.api_header)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Access the response data as JSON
            data = response.json()
            return data['number_stores']
            
        # If the request was not successful, print the status code and response text
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")
            
    def retrieve_stores_data(self):
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
        
    def extract_from_s3(self, s3_address):
        if not isinstance(s3_address, str):
            raise ValueError("s3_address must be a string containing the S3 URI")
    
        try:
            # Boto3 code that may raise exceptions
            s3 = boto3.client('s3')
            uri = str(s3_address)
            uri_split = uri.split('/')
            bucket = uri_split[2]
            key = '/'.join(uri_split[3:])
            file = f'tables & csv/{key}'
            s3.download_file(Bucket=bucket, Key=key, Filename=file)
            print("*** Data Downloaded ***")
            dataframe = pd.read_csv(file)
            print("--- DataFrame Extracted ---")
            
            # Process the response or perform other operations
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
        
        
       
    
    
        
    
         



