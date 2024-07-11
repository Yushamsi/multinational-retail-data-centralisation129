import data_extraction
import yaml
import database_utils
import pandas as pd


class DataCleaning:
    def __init__(self) -> None:
        pass
        
    def clean_user_data(self, user_df):
        print(user_df)
    
        print('Info: \n', user_df.info())
        # Check for missing values
        print("Missing values in each column:\n", user_df.isnull().sum())
        # Check for null values (should be the same as missing values for this purpose)
        print("\nNull values in each column:\n", user_df.isna().sum())
        # Check data types of each column
        print("\nData types of each column:\n", user_df.dtypes)
        # Get a statistical summary of the DataFrame
        print("\nDescription of the DataFrame:\n", user_df.describe(include='all'))

        # Check for incorrect data types (optional step, assuming we expect certain types)
        for column in user_df.columns:
            if not pd.api.types.is_numeric_dtype(df[column]):
                print(f"Warning: Column '{column}' contains non-numeric data.")
        
        # Check for incorrectly typed values and attempt to convert date columns
        for column in user_df.columns:
            # Check for numeric columns containing non-numeric data
            if user_df[column].dtype == 'object':
                try:
                    user_df[column] = pd.to_numeric(user_df[column])
                except ValueError:
                    print(f"Warning: Column '{column}' contains non-numeric data.")

            # Check for date columns
            if 'date' in column.lower() or 'time' in column.lower():
                try:
                    user_df[column] = pd.to_datetime(user_df[column], errors='coerce')
                    print(f"Converted '{column}' to datetime.")
                except Exception as e:
                    print(f"Error converting '{column}' to datetime: {e}")
        
        # Print data types of each column
        print("\nData types of each column before cleaning:\n", user_df.dtypes)
        
        # Remove rows with all null values
        user_df.dropna(how='all', inplace=True)
        
        # Filling missing values based on data type
        for column in user_df.columns:
            if user_df[column].dtype in ['int64', 'float64']:
                user_df[column].fillna(user_df[column].mean(), inplace=True)
            elif self.df[column].dtype == 'datetime64[ns]':
                self.df[column].fillna(self.df[column].mode()[0], inplace=True)
            else:
                self.df[column].fillna(self.df[column].mode()[0], inplace=True)
        
        # Check for missing values after cleaning
        print("Missing values in each column after cleaning:\n", user_df.isnull().sum())
        
        # Print data types of each column after cleaning
        print("\nData types of each column after cleaning:\n", user_df.dtypes)
        return user_df
        
    def clean_card_data(self, card):
        pass
           
    def called_clean_store_data(self, dataframe):
        pass
        
        
        
# if __name__ == '__main__':
#     table_name = 'legacy_users'
#     link_to_pdf = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

#     test = DataCleaning(yaml_file='db_creds.yaml')
#     test.clean_user_data(table_name = 'legacy_users', upload_table_name='dim_users')
    
#     test2 = DataCleaning(yaml_file='db_creds.yaml')
#     test2.clean_card_data(link = link_to_pdf, upload_table_name='dim_card_details')
        