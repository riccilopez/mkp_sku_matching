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
        except KeyError as e:
            print(f'{e} - Required columns were not found.')
            return None
        except ValueError as e:
            print(f'{e} - `date` column has the incorrect type.')
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
            'col_price':      'price',
            'col_date':       'date'
        }

    def parse_price_col(self, val: Union[str, float]) -> float:
        """
            Cleans the `price` column by removing special characters
            and returns a float.
        """
        s = re.sub('\"|\$|\,|[c/u]|[/u]', '', str(val)).strip()
        try:
            num_val = round(float(s), 2)
        except ValueError as e:
            num_val = np.nan
        return num_val
    
    def validate_url_col(self, url: str) -> str:
        """
            Validates the `url` column returning `None` in case 
            of incorrect format.
        """
        if url is None or not isinstance(url, str):
            return None
        len_url = len(url)
        if len_url > 10:
            url_pattern = re.compile(r'^https?://[\w.-]+')
            if url_pattern.match(str(url)):
                return url
            else:
                return 'https://' + url
        else: 
            return None
        
    def extract_gift_from_sku_name(self, sku_name: str) -> str:
        """
            Extracts the first element of the string after splitting it
            using the `+` character. Returns the first element and the 
            complement, which could be `None` in case 
        """
        sku_name = str(sku_name)
        tokens   = sku_name.split(' + ', maxsplit = 1)
        if len(tokens) == 1:
            tokens += ['None'] 
        return tokens

    def validate_sku_name_col(self, sku_name: str) -> str:
        """
            Validates the `sku_name` column returning `None` in case 
            of incorrect format.
        """
        len_sku_name = len(sku_name)
        num_words    = len(sku_name.split(' '))
        if len_sku_name >= 10 and num_words > 2:
            sku_name = ' '.join([s.capitalize() 
                                 if len(s) >= 3 else s.lower()
                                 for s in sku_name.split(' ')
                                ])
            return sku_name 
        else:
            return pd.NA

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
        col_date     = self.req_cols['col_date']
        col_comp     = self.req_cols['col_competitor']
        col_quantity = 'quantity'
        col_gift     = 'gift_or_extra_prod'

        # Read the CSV file using Pandas
        df = pd.read_csv(self.file_path, 
                         sep = self.sep,
                         engine = 'python',
                         on_bad_lines = 'skip',
                         encoding = 'utf-8')
        
        # 1) Verify the required columns exists
        # 2) Check if the price, name, and url columns are correct
        # 3) Check that the company column is unique
        # 4) Check the dat column is valid

        # 1) Verify that the required columns are present
        missing_columns = [col for col in self.req_cols.values() 
                            if col not in df.columns]
        if missing_columns:
            #TODO: Check if rise an AssertionError
            print(f"Missing columns: {', '.join(missing_columns)}")
            return None
        
        # Manage the `quantity` column, if exists fill with empty string
        # if not, create it and fill it with empty stings
        if col_quantity in df.columns:
            df[col_quantity] = df[col_quantity].astype('string')
            df.loc[:, col_quantity] = df[col_quantity].fillna('')
        else:
            df[col_quantity] = ''
        
        # Validate the date column
        try:
            #assert df[col_date].nunique() == 1
            df[col_date] = pd.to_datetime(df[col_date], errors = 'coerce',
                                          format = "%d-%m-%y")
        except AssertionError as e:
            print(f'{self.file_path.name} - ({type(e)}) Multiple dates were provided.',
                  df[col_date].unique())
            return None
        except ValueError as e:
            print(f'{self.file_path.name} - ({type(e)}) the `date` column is incorrect.')
            return None
        
        # In case of combo or gifts, separate the main sku (the first occurrence)
        df[[col_sku_name, col_gift]] = df[[col_sku_name]]\
                .apply(lambda row: self.extract_gift_from_sku_name(row[col_sku_name]),
                       axis = 'columns', result_type = 'expand')
        
        # Parse numeric columns `price` 
        df.loc[:, col_price]    = df[col_price].apply(self.parse_price_col)
        # Validate the `name` column
        df.loc[:, col_sku_name] = df[col_sku_name].apply(self.validate_sku_name_col)
        df = df.dropna(subset=[col_sku_name])
        # Add `quantity` data 
        df.loc[:, col_sku_name] = df[col_sku_name].str.cat(df[col_quantity], sep = ' ')
        # Validate the `url` column
        df.loc[:, col_url]      = df[col_url].apply(self.validate_url_col)
        # Replace the `zone`
        df.loc[:, 'zone']       = df['zone'].replace({
            'unique': 'Nacional'    
        })

        # Manage `specialPrice`` column
        # Check if the `SpecialPrice` exists if not, create special_price
        if 'specialPrice' in df.columns:
            df.loc[:, 'specialPrice'] = df['specialPrice'].apply(self.parse_price_col) 
        else:
            df['specialPrice'] = np.nan

        # Filter records where 'sku' or 'price' is null
        valid_df = df.dropna(subset=[col_sku_name])
        if valid_df[col_sku_name].isnull().all():
            #TODO: Check if rise an AssertionError
            print(f"{self.file_path.name} - has incorrect `name` values.")
            return None
        valid_df = df.dropna(subset=[col_price])
        if valid_df[col_price].isnull().all():
            #TODO: Check if rise an AssertionError
            print(f"{self.file_path.name} - has incorrect `price` values.")
            return None
        
        # Parse the rest of columns
        valid_df.loc[:, col_comp] = valid_df[col_comp].astype('category')
        valid_df.loc[:, col_url]  = valid_df[col_url].astype('string')
        valid_df.loc[:, col_gift] = valid_df[col_gift].astype('string')

        # Rename columns
        valid_df = valid_df.rename({
            'name':         'competitor_sku_name', 
            'company':      'competitor_name',
            'price':        'competitor_price',
            'specialPrice': 'special_price',
            'url':          'competitor_url',
            'zone':         'locality'}, axis = 1)
        
        # Return only required columns?
        if self.only_required_cols:
            valid_df = valid_df[['type', 'country', 'locality', 
                                 'date', 'competitor_name',
                                 'competitor_sku_name', 
                                 'competitor_price', 'special_price',
                                 'competitor_url', col_gift]]
        return valid_df