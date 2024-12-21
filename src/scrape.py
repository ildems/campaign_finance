import re  
import pandas as pd  
import csv
import requests
import codecs
from datetime import datetime, timedelta
import json
import io
import time
import random

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

# Helper function for snake case
def to_snake(s):
  return '_'.join(
    re.sub('([A-Z][a-z]+)', r' \1',
    re.sub('([A-Z]+)', r' \1',
    s.replace('-', ' '))).split()).lower()

def fetch_header(url):
    with requests.get(url, stream=True,headers=headers) as r:
        buffer = r.iter_lines()
        header_line = next(buffer).decode('latin-1')
        header = header_line.strip().split('\t')
    return header

def stream_data_from_url(
        url:str,
        date_field:str,
        byte_offset:int = None,
        PULL_FULL_FILE:bool = False
        ):
    start = datetime.now()

    data, chunksize = [], 10000

    headers['Range'] = None

    header = fetch_header(url)

    if byte_offset is not None:
        headers['Range'] = f'bytes={byte_offset}-'

    with requests.get(url, headers=headers, stream=True) as r:
        _break = False

        buffer = r.iter_lines()

        if byte_offset is not None:
            next(buffer)
        
        reader = csv.DictReader(codecs.iterdecode(buffer, 'latin-1'), fieldnames=header, delimiter='\t', quoting=csv.QUOTE_NONE)

        for i, row in enumerate(reader):
            if i>0:
                _break = True

                if _break == True:
                    data.append(row)
                elif i % chunksize == 0:
                    if datetime.strptime(row[date_field], '%Y-%m-%d %H:%M') >= (datetime.today() - timedelta(days=90)):
                            print(f'Break at {i} rows')
                            _break = True
                
        
        print(f'Data read in {datetime.now() - start}')
        df = pd.DataFrame(data)

        if None in df.columns:
            df = df.drop(columns=[None])

        print(url)
        print(df)
        
        df.columns = [to_snake(x) for x in df.columns]

        return df

# For parsing data directly from the board of elections website. Should output a dataframe.
def data_from_url(url):
    # Import
    retry_delay = 30  # Initial delay in seconds
    max_retries = 3
    for attempt in range(max_retries):
        try:
            urlData = requests.get(url,headers=headers).content
            df = pd.read_csv(io.StringIO(urlData.decode('latin-1')), sep='\t', encoding='latin-1')
            
            # Columns to snake case
            df.columns = [to_snake(x) for x in df.columns]
        except Exception as e:
            print(f"Retry read {url} write number {attempt+1}")
            print(e)
            time.sleep(retry_delay)
            retry_delay *= (attempt+1)  # Double the delay for the next attempt
            retry_delay += random.uniform(0, 1)  # Add jitter

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
            "data":stream_data_from_url(l['link'],l['date_field'], 
                                        byte_offset = l['byte_offset']
                                        # byte_offset=None
                                        ),
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