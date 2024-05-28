# DPI Board of Elections Scraper

This repo is set up to regulary read, clean, and move data from the Illinois Board of Election website and BigQuery

Currently set up to run locally, will eventually run in DNC's Portal

## Local setup
### Install virtualenv
Run `pip install virtualenv` (or if you have multiple versions of python running, `py -3.9 -m pip install virtualenv`)

### Create a virtual environment
In the folder with your repo, run `python venv .venv` (or if you have multiple version fo python running `py -3.9 -m venv .venv`)

This will create a new folder in your repo called .venv with the virtual environment in it

### Using the virutal environment
On Windows, activate the virtual enviroment by running the command `.venv\Scripts\activate.bat` (the backslashes matter)
On Mac/Linux, activate the virtual enviroment by running the command `source .venv/bin/activate`

On first run, run the command `python -m pip install -r requirements.txt` to install all of the required packages from the requirements.txt file in the repo

When you want to run the script, just run `python main.py`

#### Note on package versions
After the first run, you'll be able to run scripts from the virtual environment without reinstalling anything after you activate

### .env file and credentials
To run locally, you can authenticate using the GCloud CLI with the default-application flag

In command line, run `gcloud auth application-default login` and make sure you are in your demsdata.org account when granting permission

Set your GCloud CLI project using `gcloud config set projectid demsilsp`

Create .env file in the main folder of your repo with:
    `ENVIROMENT=dev`

### Smaller BOE Files
Run `python main.py boe_scrape` to execute code in command line
Data from files in the links.json file will be moved to BQ

### Larger BOE Files
STILL IN DEV
Run `python main.py boe_stream` to execute code in command line
Data will be streamed from links_stream.json
Data will be read for most recent updates before loading into BQ
