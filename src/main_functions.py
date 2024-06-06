from src.bq import data_to_bq
from src.cleanup import clean_data
from src.scrape import scrape_boe,stream_boe

from datetime import datetime as _datetime
from google.cloud import bigquery
import glob
import os
import pandas as pd
import pathlib
import json

# Function to run locally for testing
# Arguments set where the data is pulling from and which files are being scraped

def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def main_local(
    raw_data_folder:os.PathLike = pathlib.Path('data', 'raw'),
    cleaned_data_folder:os.PathLike = pathlib.Path('data', 'cleaned'),
    WRITE_LOCAL:bool = True,
    READ_LOCAL:bool = False,
    WRITE_CLOUD:bool = False,
    scrape_data:bool = True,
    stream_data:bool = True
):
    
    data = []
    byte_offsets = {}

    if WRITE_LOCAL:
        ensure_directory_exists(raw_data_folder)
        ensure_directory_exists(cleaned_data_folder)

        if scrape_data:
            data = data + scrape_boe(pathlib.Path('links', 'links.json'))
        
        if stream_data:
            data = data + stream_boe(pathlib.Path('links', 'links_stream.json'))

        for d in data:
            print(d['data'])
            d['data'].to_csv(pathlib.Path(raw_data_folder,d['name']+'.csv'),index=False)

        for d in data:
            clean_data(d).to_csv(pathlib.Path(cleaned_data_folder,d['name']+'.csv'),index=False)


    if READ_LOCAL:
        csv_files = glob.glob(os.path.join(raw_data_folder, "*.csv"))

        for f in csv_files:
            data.append({
                'data':pd.read_csv(f),
                'name':f.split("\\")[-1]
            })

    else:
        if scrape_data:
            data.append(scrape_boe)
        
        if stream_data:
            data.append(stream_boe)

    if WRITE_CLOUD:
        client = bigquery.Client()

        for d in data:
            data_to_bq(client,clean_data(d['data']),'demsilsp','boe_stream',d['table'])
    
    

# Function for running in production on DNC's Portal

# def main_live(

# ):
    



# try:
#         data_to_bq(client,data,'demsilsp','boe_stream',l['table'])    
#         print(f"{len(data)} rows loaded into {l['table']} at f{_datetime.now() - start}")
#     except Exception as e:
#         print(e)
#         print(f"Failed load into {l['table']} at f{_datetime.now() - start}")