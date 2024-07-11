import yaml
import database_utils
import pandas as pd
import tabula
import requests

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
                print(store_data)
                data_dict = pd.DataFrame(store_data, index=[store_number])
                print(data_dict)
                all_stores_data.append(data_dict)
                print(data_dict)
                
            except requests.RequestException as e:
                print(f"Error retrieving data for store {store_number}: {e}")

        # Concatenate all DataFrame objects into a single DataFrame
        if all_stores_data:
            print(all_stores_data)
            store_dataframe = pd.concat(all_stores_data, ignore_index=True)
            return store_dataframe
        else:
            return pd.DataFrame()


            

# if __name__ == '__main__':
#     no_stores = DataExtractor()
#     # no_stores.list_number_of_stores()    
#     data = no_stores.retrieve_stores_data()  
    
#     data.to_csv('stores_data.csv')
        
        
        
       
    
    
        
    
         



