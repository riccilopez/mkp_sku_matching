import os
import re
import pandas as pd
from pathlib import Path
from typing import Union

class CSVFileReader:
    """
        A class for reading and validating CSV files obtained from webscraping.
    """
    def __init__(self, file_path: Path, 
                 only_required_cols: bool = True) -> None:
        self.file_path = file_path
        self.only_required_cols = only_required_cols
        self.req_cols = {
            'col_sku_name':   'Category', 
            'col_competitor': 'Store',
            'col_url':        'Url',
            'col_quantity':   'Quantity', 
            'col_price':      'Price'
        }


    def parse_price_col(self, val: str) -> float:
        """
            Cleans the `price` column by removing special characters
            and returns a float.
        """
        s = re.sub('\"|\$|\,|[c/u]|[/u]', '', str(val)).strip()
        try:
            num_val = float(s)
        except ValueError as e:
            num_val = 0
        return num_val


    def validate_file(self) -> Union[pd.DataFrame, None]:
        """
            Validates the CSV file specified by file_path.
        """
        # Check if the file exists and it is not empty
        if not os.path.exists(self.file_path):
           return None 
        if os.path.getsize(self.file_path) == 0:
            print(f"The file {self.file_path.name} is empty or not a valid CSV file.")
            return None

        # Define the required columns
        col_sku_name = self.req_cols['col_sku_name']
        col_price    = self.req_cols['col_price']
        col_quantity = self.req_cols['col_quantity']

        # Read the CSV file using Pandas
        df = pd.read_csv(self.file_path, encoding='utf-8')

        # Verify that the required columns are present
        missing_columns = [col for col in self.req_cols.values() 
                            if col not in df.columns]
        if missing_columns:
            print(f"Missing columns: {', '.join(missing_columns)}")
            return None

        # Filter records where 'sku' or 'price' is null
        valid_df = df.dropna(subset=[col_price, col_price])

        # Parse numeric columns `Price` and `Quantity`
        valid_df.loc[:, col_price] = valid_df[col_price].apply(self.parse_price_col)
        # Include the quantity value into the sku_name
        valid_df.loc[:, col_sku_name] = valid_df[col_sku_name] + " " +\
                                        valid_df[col_quantity].fillna('') 

        # TODO: Temp step to keep only one segment of prices 
        #  if the `Estado` column is present.
        # ************************************
        if 'Estado' in valid_df.columns:
            # Keep the most frequent estado
            most_freq_estado = valid_df.value_counts('Estado', ascending=False).index[0]
            valid_df = valid_df[valid_df['Estado'] == most_freq_estado]\
                        .reset_index(drop = True)
        # ************************************
        
        # Return only required columns?
        if self.only_required_cols:
            valid_df = valid_df[(self.req_cols.values())].drop(col_quantity, axis = 1)

        return valid_df