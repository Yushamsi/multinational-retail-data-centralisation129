import re

import numpy as np
import pandas as pd
import yaml
from pprint import pprint

import database_utils
import data_extraction


class DataCleaning:
    def __init__(self) -> None:
        pass
        
    def clean_user_data(self, user_df): # specific to legacy_users
        print('Setting Index')
        user_df.set_index('index', inplace=True)
        
        print('Info: \n', user_df.info())
        # Check for missing values
        print("Missing values in each column:\n", user_df.isnull().sum())
        # Check for null values (should be the same as missing values for this purpose)
        print("\nNull values in each column:\n", user_df.isna().sum())
        # Check data types of each column
        print("\nData types of each column:\n", user_df.dtypes)
        # Get a statistical summary of the DataFrame
        print("\nDescription of the DataFrame:\n", user_df.describe(include='all'))
        
        # Remove rows with all null values
        user_df.dropna(how='all', inplace=True)
        
        user_df['country_code'] = user_df['country_code'].astype(str).replace('GGB', 'GB')
        
        country_code_values=["GB", "DE", "US"]
        user_df =  user_df[user_df["country_code"].isin(country_code_values)]

        # Check for incorrect data types (optional step, assuming we expect certain types)
        for column in user_df.columns:
            if not pd.api.types.is_numeric_dtype(user_df[column]):
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
        
        # Filling missing values based on data type
        for column in user_df.columns:
            if user_df[column].dtype in ['int64', 'float64']:
                user_df[column].fillna(user_df[column].mean(), inplace=True)
            elif user_df[column].dtype == 'datetime64[ns]':
                user_df[column].fillna(user_df[column].mode()[0], inplace=True)
            else:
                user_df[column].fillna(user_df[column].mode()[0], inplace=True)
        
        # Check for missing values after cleaning
        print("\nMissing values in each column after cleaning:\n", user_df.isnull().sum())
        
        # Print data types of each column after cleaning
        print("\nData types of each column after cleaning:\n", user_df.dtypes)
        return user_df
        
    def clean_card_data(self, card):
        print('Info: \n')
        card.info()
        
        # Check for missing values
        print("\nMissing values in each column:\n", card.isnull().sum())
        # Check for null values (should be the same as missing values for this purpose)
        print("\nNull values in each column:\n", card.isna().sum())
        # Check data types of each column
        print("\nData types of each column:\n", card.dtypes)
        # Get a statistical summary of the DataFrame
        print("\nDescription of the DataFrame:\n", card.describe(include='all'))
        
        card_provider_values=['Diners Club / Carte Blanche', 'American Express', 'JCB 16 digit',
       'JCB 15 digit', 'Maestro', 'Mastercard', 'Discover',
       'VISA 19 digit', 'VISA 16 digit', 'VISA 13 digit']

        card =  card[card["card_provider"].isin(card_provider_values)]
        
        # Remove rows with all null values
        card.dropna(how='all', inplace=True)
                    
        card = card.drop_duplicates()
        
        card['card_number'] = card['card_number'].astype(str).str.replace('?', '', regex=False)

        
        # Check for missing values after cleaning
        print("\nMissing values in each column after cleaning:\n", card.isnull().sum())
        
        # Print data types of each column after cleaning
        print("\nData types of each column after cleaning:\n", card.dtypes)
        return card
            
    def clean_store_data(self, df):
        df.set_index('index', inplace=True)

        # Display info about the DataFrame
        print('\nInfo: \n')
        df.info()
        
        # Check for missing values
        print("\nMissing values in each column:\n", df.isnull().sum())
        # Check for null values (should be the same as missing values for this purpose)
        print("\nNull values in each column:\n", df.isna().sum())
        # Check data types of each column
        print("\nData types of each column:\n", df.dtypes)
        # Get a statistical summary of the DataFrame
        print("\nDescription of the DataFrame:\n", df.describe(include='all'))
        
        df.drop(columns=['lat'], inplace=True)
        
        df['continent'] = df['continent'].astype(str).replace('eeEurope', 'Europe')
        df['continent'] = df['continent'].astype(str).replace('eeAmerica', 'America')
        
        continent_values=['Europe', 'America']
        df = df[df["continent"].isin(continent_values)]
        df.continent.unique()
        
        df['staff_numbers'] = df['staff_numbers'].apply(lambda x: re.sub(r'[a-zA-Z]', '', x))
        
        # Remove rows where the majority of values are null
        threshold = len(df.columns) / 2
        df.dropna(thresh=threshold, inplace=True)
        print(f"\nDataFrame after removing rows with more than {threshold} null values:\n", df.head())
        
        # Identify and convert data types
        for column in df.columns:
            if 'date' in column.lower() or 'time' in column.lower():
                try:
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                    print(f"Converted '{column}' to datetime.")
                except ValueError:
                    print(f"Warning: Could not convert '{column}' to datetime.")
            elif df[column].dtype == 'object':
                if df[column].str.replace('.', '', 1).str.isnumeric().all():
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                    print(f"Converted '{column}' to numeric.")
                else:
                    df[column] = df[column].astype('string')
                    print(f"Ensured '{column}' is treated as string.")
        
        # Fill missing values
        for column in df.columns:
            if df[column].dtype in ['int64', 'float64']:
                df[column].fillna(df[column].mean(), inplace=True)
            elif df[column].dtype == 'datetime64[ns]':
                df[column].fillna(df[column].mode()[0], inplace=True)
            else:
                df[column].fillna(df[column].mode()[0], inplace=True)
        
        # Check for missing values after cleaning
        print("\nMissing values in each column after cleaning:\n", df.isnull().sum())
        
        # Print data types of each column after cleaning
        print("\nData types of each column after cleaning:\n", df.dtypes)
        
        return df
    
    def convert_product_weights(self, products):
        # Helper function to convert weights to kg
        def convert_to_kg(weight):
            # Initialize quantity to 1 by default (used if no 'x' is present or if weight is a float)
            quantity = 1

            # Ensure the weight is a string before trying to manipulate it
            if isinstance(weight, str):
                # Normalize the weight string by removing commas and stripping whitespace
                weight = weight.replace(',', '').strip()

                # Check for 'x' indicating multiple items
                if 'x' in weight:
                    parts = weight.split('x')
                    if len(parts) == 2:
                        quantity = float(parts[0].strip())  # Quantity of items
                        weight = parts[1].strip()  # Weight per item
                    else:
                        return None  # Malformed weight string

                # Find the transition from numbers to letters
                num_part = ''
                unit_part = ''
                numeric_found = False
                for char in weight:
                    if char.isdigit() or char == '.':
                        num_part += char
                        numeric_found = True
                    elif numeric_found:  # start adding to unit_part only after numbers are found
                        unit_part += char

                if num_part:
                    num = float(num_part) * quantity  # Multiply by quantity if applicable
                else:
                    return None  # Return None if no numeric part is found
            else:
                num = float(weight) * quantity  # If it's already a number, use it directly assuming quantity was handled

            # Convert unit part to lowercase and strip any surrounding spaces
            unit = unit_part.strip().lower() if isinstance(weight, str) else 'kg'

            # Determine the unit and convert to kilograms
            if 'kg' in unit:
                return num
            elif 'g' in unit:
                return num / 1000  # Convert grams to kg
            elif 'mg' in unit:
                return num / 1000000  # Convert mg to kg
            elif 'ml' in unit:
                return num / 1000  # Assume 1 ml = 1 g
            elif 'l' in unit:
                return num  # 1 liter of water assumed to be equivalent to 1 kg
            return num  # Default case, no recognizable unit, assume kg

        # Apply the conversion function to the weight column
        products['weight'] = products['weight'].apply(convert_to_kg)
        
        return products
    
    def clean_products_data(self, df):
        # Display the initial DataFrame
        print("Initial DataFrame:\n", df.head())

        # Display info about the DataFrame
        print('\nInfo: \n')
        df.info()

        # Check for missing values
        print("\nMissing values in each column:\n", df.isnull().sum())
        # Check for null values (should be the same as missing values for this purpose)
        print("\nNull values in each column:\n", df.isna().sum())
        # Check data types of each column
        print("\nData types of each column:\n", df.dtypes)
        # Get a statistical summary of the DataFrame
        print("\nDescription of the DataFrame:\n", df.describe(include='all'))
        
        removed_values=['Still_avaliable', 'Removed']
        df = df[df["removed"].isin(removed_values)]
        df.removed.unique()

        # Remove rows where the majority of values are null
        threshold = len(df.columns) / 2
        df.dropna(thresh=threshold, inplace=True)
        print(f"\nDataFrame after removing rows with more than {threshold} null values:\n", df.head())

        # Drop the unnamed column if it exists
        df.drop(columns=[col for col in df.columns if 'unnamed' in col.lower()], errors='ignore', inplace=True)
        print("\nDataFrame after removing 'Unnamed' columns:\n", df.head())
        
        return df
    
    def clean_orders_data(self, order_df):
        # Display info about the DataFrame
        print('\nInfo: \n')
        order_df.info()
        
        columns_to_remove = ['first_name', 'last_name', '1', 'level_0']
        order_df.drop(columns=['first_name', 'last_name', '1', 'level_0'], inplace=True)
        print(f"\nRemoved {columns_to_remove}:\n")
        return order_df
    
    def clean_date_data(self, date_data):
        values=['Evening', 'Morning', 'Midday', 'Late_Hours']
        date_data = date_data[date_data['time_period'].isin(values)]
        return date_data
        
        
if __name__ == '__main__':
    products_df = pd.read_csv("tables & csv/products.csv")
    clean = DataCleaning()
    conv_products_df = clean.convert_product_weights(products=products_df)
    print(conv_products_df['weight'])
    clean_products = clean.clean_products_data(conv_products_df)
    
        