import pandas as pd
from pathlib import Path
#from sodapy import Socrata
from datetime import datetime
import requests
import os

def extract_data(target_file, date, start_date):
    target_dir = os.path.dirname(target_file)

    os.makedirs(target_dir, exist_ok=True)
    url = "https://data.austintexas.gov/resource/9t4d-g238.csv"
    params = {
        "$$app_token": "oexSFfxU4eWZ6EFLCCAMWxKZe",
    }
    #Path(target_dir).mkdir(parents=True, exist_ok=True)
    #CSV_TARGET_FILE =  target_dir + f'/outcomes_{date}.csv'
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.text
        with open(target_file, 'w') as file:
            file.write(data)
        
