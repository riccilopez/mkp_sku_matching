import os
import re
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Union

class MktpPricesFileReader:
    """
        A class for reading and validating `.parquet` files 
        containing the sku prices of MAZ Marketplace.
    """
    def __init__(self, file_path: Path, 
                 country: str = None) -> None:
        if not isinstance(file_path, Path):
            file_path  = Path(file_path)
        if country == None:
            country = file_path.name[:2]
        self.country      = country
        self.file_path    = file_path
        self.valid_colums = ['date', 'region_name', 
                             'sku', 'sku_name', 'price']
        
    def validate_country_name(self):
        # TODO: move list of countries to a dictionary
        valid_countries = ['MX', 'CO', 'PE', 'EC', 'PA', 'SV', 'HN']
        return self.country in valid_countries
        
    def read_file(self) -> Union[pd.DataFrame, None]:
        try:
            df = pd.read_parquet(self.file_path)
            df = df[self.valid_colums]
            # Check the column date is correct
            df['date']  = pd.to_datetime(df['date'])
            # Remove leading zeros
            df['sku']   = df['sku'].str.lstrip('0')
            # Format the price column
            df['price'] = df['price'].astype(float).round(2)
        except FileNotFoundError as e:
            print(f'{e} - The `{self.file_path.name}` file was not found.')
            return None
        except KeyError:
            print(f'{e} - Required columns were not found.')
            return None
        except DateParseError:
            print(f'{e} - `date` column has the incorrect type.')
            return None
        except ValueError:
            print(f'{e} - the given columns have not the correct type.')
            return None
        else:
            # Include the country column
            if not self.validate_country_name():
                print(f'{self.country} is a invalid country name.')
                return None
            df.insert(loc = 0, column = 'country', value = self.country)
            # Drop duplicates
            df = df.drop_duplicates(subset = ['date', 'sku', 'region_name'])
            # Drop null values
            df = df.dropna()
            return df



class OnlineFileReader:
    """
        A class for reading and validating plain text 
        files obtained from webscraping.
    """
    def __init__(self, 
                 file_path: Path, 
                 sep: str = '<s>',
                 only_required_cols: bool = True) -> None:
        if not isinstance(file_path, Path):
            file_path = Path(file_path)
        self.file_path = file_path
        self.sep = sep
        self.only_required_cols = only_required_cols
        self.req_cols = {
            'col_sku_name':   'name', 
            'col_competitor': 'company',
            'col_url':        'url',
            'col_price':      'price'
        }

    def parse_price_col(self, val: Union[str, float]) -> float:
        """
            Cleans the `price` column by removing special characters
            and returns a float.
        """
        s = re.sub('\"|\$|\,|[c/u]|[/u]', '', str(val)).strip()
        try:
            num_val = float(s)
        except ValueError as e:
            num_val = np.nan
        return num_val
    
    def validate_url_col(self, url: str) -> str:
        """
            Validates the `url` column returning `np.nan` in case 
            of incorrect format.
        """
        url_pattern = re.compile(r'^https?://[\w.-]+')
        if url_pattern.match(str(url)):
            return url
        else:
            return np.nan
        
    def extract_gift_from_sku_name(self, sku_name: str) -> str:
        """
            Extracts the first element of the string after splitting it
            using the `+` character. Returns the first element and the 
            complement, which could be `None` in case 
        """
        sku_name = str(sku_name)
        tokens   = sku_name.split(' + ', maxsplit = 1)
        if len(tokens) == 1:
            tokens += [np.nan] 
        return tokens

    def validate_sku_name_col(self, sku_name: str) -> str:
        """
            Validates the `sku_name` column returning `np.nan` in case 
            of incorrect format.
        """
        len_sku_name = len(sku_name)
        num_words    = len(sku_name.split(' '))
        if len_sku_name >= 10 and num_words > 2:
            return sku_name 
        else:
            return np.nan

    def read_file(self) -> Union[pd.DataFrame, None]:
        """
            Validates the given file specified by file_path.
        """
        # Check if the file exists and it is not empty
        if not os.path.exists(self.file_path):
            return None 
        if os.path.getsize(self.file_path) == 0:
            print(f"The file {self.file_path.name} is empty or not a valid file.")
            return None

        # Define the required columns
        col_sku_name = self.req_cols['col_sku_name']
        col_price    = self.req_cols['col_price']
        col_url      = self.req_cols['col_url']
        col_gift     = 'gift_or_extra_prod'

        # Read the CSV file using Pandas
        df = pd.read_csv(self.file_path, 
                         sep = self.sep,
                         engine = 'python',
                         encoding = 'utf-8')
        
        # 1) Verify the required columns exists
        # 2) Check if the price, name, and url columns are correct
        # 3) Check that the company column is unique

        # 1) Verify that the required columns are present
        missing_columns = [col for col in self.req_cols.values() 
                            if col not in df.columns]
        if missing_columns:
            #TODO: Check if rise an AssertionError
            print(f"Missing columns: {', '.join(missing_columns)}")
            return None
        
        # In case of combo or gifts, separate the main sku (the first)
        df[[col_sku_name, col_gift]] = df[[col_sku_name]]\
                .apply(lambda row: self.extract_gift_from_sku_name(row[col_sku_name]),
                       axis = 'columns', result_type = 'expand')
        
        # Parse numeric columns `price` 
        df.loc[:, col_price]    = df[col_price].apply(self.parse_price_col)
        # Parse numeric columns `name` 
        df.loc[:, col_sku_name] = df[col_sku_name].apply(self.validate_sku_name_col)
        # Parse numeric columns `url` 
        df.loc[:, col_url]      = df[col_url].apply(self.validate_url_col)
        # Filter records where 'sku' or 'price' is null
        valid_df = df.dropna(subset=[col_sku_name, col_price])
        if len(valid_df) == 0:
            #TODO: Check if rise an AssertionError
            print(f"The Dataframe ({self.file_path.name})",
                   "has incorrect `price` and/or `name` values.")
            return None
        # Rename columns
        valid_df = valid_df.rename({
            'name':    'competitor_sku_name', 
            'company': 'competitor_name',
            'price':   'competitor_price',
            'url':     'competitor_url',
            'zone':    'region'}, axis = 1)
        
        # Return only required columns?
        if self.only_required_cols:
            valid_df = valid_df[['type', 'country', 'region', 
                                 'date', 'competitor_name',
                                 'competitor_sku_name', 'competitor_price', 
                                 'competitor_url', col_gift]]
        return valid_df