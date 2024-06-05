import re  
import pandas as pd  
import csv
import requests
import codecs
from datetime import datetime, timedelta

# Helper function for snake case
def to_snake(s):
  return '_'.join(
    re.sub('([A-Z][a-z]+)', r' \1',
    re.sub('([A-Z]+)', r' \1',
    s.replace('-', ' '))).split()).lower()

def estimate_row_size(url, sample_size = 100):
    with requests.get(url, stream=True) as r:
        buffer = r.iter_lines()
        reader = csv.DictReader(codecs.iterdcode(buffer, 'latin-1'), delimiter='\t', quoting=csv.QUOTE_NONE)
        total_size = 0
        row_count = 0

        for row in reader:
            row_count += 1
            row_str = '\t'.join(row.values())
            total_size += len(row_str.encode('latin-1'))
            if row_count >= sample_size:
                break

    average_row_size = total_size / row_count if row_count > 0 else 0

    return average_row_size

def stream_data_from_url(url, rows_to_read, average_row_size):
    start_time = datetime.now()
    with requests.head(url) as r:
        file_size = int(r.headers.get('Content-Length', 0))
        print(f"File size: {file_size} bytes")

    approximate_position = max(file_size - (rows_to_read * average_row_size), 0)
    print(f"Starting read from byte position: {approximate_position}")

    headers = {'Range': f'bytes={approximate_position}-'}

    with requests.get(url, stream=True) as r:
        buffer = r.iter_lines()  # iter_lines() will feed you the distant file line by line
        reader = csv.DictReader(codecs.iterdecode(buffer, 'latin-1'), delimiter='\t', quoting=csv.QUOTE_NONE)

        data = []
       
        for i,row in enumerate(reader):
            data.append(row)
            if len(data) > rows_to_read:
                data.pop(0)
        
        print(f'Data read in {datetime.now() - start_time}')
       
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
