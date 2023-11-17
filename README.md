# DPI Board of Elections Scraper

This repo is set up to regulary read, clean, and move data from the Illinois Board of Election website and BigQuery

Currently set up to run locally, will eventually run in DNC's Portal

## Local setup
Run `pip install -r requirements.txt` to set up dependencies

### Smaller BOE Files
Run `python main.py boe_scrape` to execute code in command line
Data from files in the links.json file will be moved to BQ

### Larger BOE Files
STILL IN DEV
Run `python main.py boe_stream` to execute code in command line
Data will be streamed from links_stream.json
Data will be read for most recent updates before loading into BQ
