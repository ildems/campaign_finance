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

def stream_data_from_url(url,chunksize,key):
    start = datetime.now()

    data, chunksize = [], 10000

    with requests.get(url, stream=True) as r:
        _break = False

        buffer = r.iter_lines()  # iter_lines() will feed you the distant file line by line
        reader = csv.DictReader(codecs.iterdecode(buffer, 'latin-1'), delimiter='\t', quoting=csv.QUOTE_NONE)
        for i,row in enumerate(reader):
            if _break == True:
                data.append(row)
            elif i % chunksize == 0:
                if (i % chunksize == 0 and i > 4000000):
                    print(f'Break at {i} rows')
                    if datetime.strptime(row['ExpendedDate'], '%Y-%m-%d  %H:%M:%S') >= (datetime.today() - timedelta(days=90)):
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