from dependencies.bq import data_to_bq
from dependencies.cleanup import cast_col,cleanup
from dependencies.stream import data_from_url,stream_data_from_url

from datetime import datetime as _datetime
from google.cloud import bigquery 
import json
import sys


def stream_boe():
    start = _datetime.now()

    # Dictionary of all the relevent links.
    link_f = open('links.json')
    link_json = link_f.read()
    links = json.loads(link_json)

    client = bigquery.Client()

    for l in links:
        print(l)
        data = stream_data_from_url(l['link'])
        try:
            data_to_bq(client,data,'demsilsp','boe_stream',l['table'])
            print(f"{len(data)} rows loaded into {l['table']} at f{_datetime.now() - start}")
        except Exception as e:
            print(e)
            print(f"Failed load into {l['table']} at f{_datetime.now() - start}")


def scrape_boe():
    start = _datetime.now()

    # Dictionary of all the relevent links.
    link_f = open('links.json')
    link_json = link_f.read()
    links = json.loads(link_json)

    client = bigquery.Client()

    for l in links:
        print(l)
        data = data_from_url(l['link'])

        for c in l['cast_fields'].keys():
            data[c] = data[c].astype(l['cast_fields'][c])

        try:
            data_to_bq(client,data,'demsilsp','boe_stream',l['table'],replace=True)
            print(f"{len(data)} rows loaded into {l['table']} at {_datetime.now() - start}")
        except Exception as e:
            print(e)
            print(f"Failed load into {l['table']} at {_datetime.now() - start}")




if __name__ == "__main__":
    args = sys.argv
    # args[0] = current file
    # args[1] = function name
    # args[2:] = function args : (*unpacked)
    globals()[args[1]](*args[2:])
