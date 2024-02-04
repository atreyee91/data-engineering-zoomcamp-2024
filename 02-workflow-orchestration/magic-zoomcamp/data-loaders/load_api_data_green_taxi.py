import io
import pandas as pd
import requests
import os
from datetime import datetime
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-"
    dfs = []

    for i in range(10,13):
        url1 = url + str(i) + ".csv.gz" 

        #for filename in filenames:
        print(f'{url1}')
        parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
        df = pd.read_csv(url1, sep=",", compression="gzip", parse_dates=parse_dates)
        
        #print(f"Number of rows in DataFrame {i}: {len(df)}")  # Debug print
        dfs.append(df)      
        #print(f"Columns of DataFrame {i}: {df.columns}")  
        
        

    combined_df = pd.concat(dfs, ignore_index=True)  # Concatenate the DataFrame       
      

    return combined_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
