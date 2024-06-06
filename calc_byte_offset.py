import json
import requests
import csv
import codecs
from datetime import datetime


def calculate_byte_offset(url, date_field, year='2024'):
    with requests.get(url, stream=True) as r:
        buffer = r.iter_lines()
        
        header_line = next(buffer).decode('latin-1')
        header = header_line.strip().split('\t')
        
        reader = csv.DictReader(codecs.iterdecode(buffer, 'latin-1'), fieldnames=header, delimiter='\t', quoting=csv.QUOTE_NONE)
        
        for line in buffer:
            decoded_line = line.decode('latin-1')
            row = next(reader)
            date_str = row.get(date_field)
            if date_str:
                try:
                    if datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').year == int(year):
                        byte_offset = r.raw.tell() - len(decoded_line) - 1
                        return byte_offset  
                except ValueError as e:
                    print(f"Skipping row with malformed date: {date_str}")
                    continue 
    return None

links_path = 'links/links_stream.json'
with open(links_path) as f:
    links = json.load(f)

for l in links:
    print(f"Calculating byte offset for {l['table']}")
    l['byte_offset'] = calculate_byte_offset(l['link'], l['date_field'], year="2024")

with open(links_path, 'w') as f:
    json.dump(links, f, indent=4)

print("Byte offsets have been calculated and updated in links_stream.json")
