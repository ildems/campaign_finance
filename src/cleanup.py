from nameparser import HumanName
from nameparser.config import CONSTANTS
from i18naddress import InvalidAddressError, normalize_address
import json
import pandas as pd
import pathlib

def clean_data(d):   
    df = d['data']
    cast_fields = d['cast_fields']
    
    df = cast_col(df, cast_fields)

    return df

def cast_col(df, mapping):
    if df.empty: 
        return df
    else:       
        if mapping:
            for col, dtype in mapping.items():
                if dtype == 'string':
                    df[col] = df[col].astype(str)
                elif dtype == 'date':
                    df[col] = pd.to_datetime(df[col], errors = 'coerce')
                    df[col] = df[col].dt.strftime('%Y-%m-%d')

        return df    
    