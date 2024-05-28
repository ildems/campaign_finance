import os
import pathlib
import src.main_functions
from dotenv import load_dotenv

load_dotenv()

if __name__ == '__main__':
    if os.environ['ENVIRONMENT'] == 'dev':

        src.main_functions.main_local(
            raw_data_folder=pathlib.Path('data', 'raw'),
            cleaned_data_folder=pathlib.Path('data', 'cleaned'),
            WRITE_LOCAL=True,
            READ_LOCAL=False,
            scrape_data=False,
            stream_data=True
        )
    
    elif os.environ['ENVIRONMENT'] == 'prod':
        src.main_functions.main_live()
    
    else:
        print("**Set .env key ENVIRONMENT to value dev or prod to run**")
