import re  
import pandas as pd  
import csv
import requests
import codecs
from datetime import datetime, timedelta
import json

# Helper function for snake case
def to_snake(s):
  return '_'.join(
    re.sub('([A-Z][a-z]+)', r' \1',
    re.sub('([A-Z]+)', r' \1',
    s.replace('-', ' '))).split()).lower()

def stream_data_from_url(
        url:str,
        date_field:str,
        PULL_FULL_FILE:bool = False
        ):
    start = datetime.now()

    data, chunksize = [], 10000

    with requests.get(url, stream=True) as r:
        _break = False

        buffer = r.iter_lines()  
        reader = csv.DictReader(codecs.iterdecode(buffer, 'latin-1'), delimiter='\t', quoting=csv.QUOTE_NONE)
        for i,row in enumerate(reader):
            if _break == True:
                data.append(row)
            elif i % chunksize == 0:
                if datetime.strptime(row[date_field], '%Y-%m-%d  %H:%M:%S') >= (datetime.today() - timedelta(days=90)):
                    print(f'Break at {i} rows')
                    _break = True
        
        print(f'Data read in {datetime.now() - start}')
        df = pd.DataFrame(data)
        
        df.columns = [to_snake(x) for x in df.columns]

        return df


# For parsing data directly from the board of elections website. Should output a dataframe.
def data_from_url(url):    
    # Import
    df = pd.read_csv(url, sep='\t', encoding='latin-1')

    # Columns to snake case
    df.columns = [to_snake(x) for x in df.columns]

    # Return
    return df

# Stream partial files from BOE website (for large files)
def stream_boe(links_path):
    # Dictionary of all the relevent links.
    link_f = open(links_path)
    link_json = link_f.read()
    links = json.loads(link_json)

    data = []

    for l in links:
        data.append({
            "name":l['table'],
            "data":stream_data_from_url(l['link'],l['date_field']),
            "cast_fields":l['cast_fields']
        })
    
    return data

# Scrape full file from BOE website
def scrape_boe(links_path):
    # Dictionary of all the relevent links.
    link_f = open(links_path)
    link_json = link_f.read()
    links = json.loads(link_json)

    data = []

    for l in links:
        print(f"Scraping {l['table']}")
        data.append({
            "name":l['table'],
            "data":data_from_url(l['link']),
            "cast_fields":l['cast_fields']
        })
    
    return data
