from nameparser import HumanName
from nameparser.config import CONSTANTS
from i18naddress import InvalidAddressError, normalize_address
import json
import pandas as pd
import pathlib

def clean_data(d):   
    df = d['data']
    table_name = d['name']
    
    df = cast_col(df, table_name)

    return df

def cast_col(df, table_name):
    if df.empty: 
        return df
    else:
        with open(pathlib.Path('links', 'links.json'), 'r') as f:
            links = json.load(f)

        with open(pathlib.Path('links', 'links_stream.json'), 'r') as f:
            links_stream = json.load(f)
        
        all_links = links + links_stream

        mapping = None
        for item in all_links:
            if item['table'] == table_name:
                mapping = item['cast_fields']
        
        if mapping:
            for col, dtype in mapping.items():
                if dtype == 'string':
                    df[col] = df[col].astype(str)
                elif dtype == 'date':
                    df[col] = pd.to_datetime(df[col], errors = 'coerce')

        return df    
    
