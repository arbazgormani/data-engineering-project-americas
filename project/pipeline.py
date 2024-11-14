import pandas as pd
import kagglehub
from sqlalchemy import create_engine
import os

from config import *


def fetch_csv(url):
    """
    Downloads CSV file from a given URL.
    """
    path = kagglehub.dataset_download(url)

    print("Path to dataset files:", path)
    csv_file_path = path + f'/{os.listdir(path)[0]}'  # Replace with your CSV file's name
    df = pd.read_csv(csv_file_path)
    return df
        
def clean_data(df):
    """
    Clean data and modfiy column names properly.
    """
    # Drop missing values for simplicity
    df.dropna(inplace=True)

    # standardize column names
    df.columns = df.columns.str.lower().str.replace(" (%) ", "_").str.replace("state/area", "state").str.replace(" ", "_")
    print(df.columns)
    
    return df


def extract_data(unemployment_data_source, crime_data_source):
    """
    Extract data from sources.
    """
    unemployment_df = fetch_csv(unemployment_data_source)
    crime_df = fetch_csv(crime_data_source)
    
    return {'unemployment_dataset': unemployment_df, 'crime_dataset': crime_df}
    

def transform_data(data_dict):
    unemployment_df = clean_data(data_dict['unemployment_dataset'])
    crime_df = clean_data(data_dict['crime_dataset'])
    # print(unemployment_df.shape, crime_df.shape)
    # data = pd.merge(unemployment_df, crime_df, on='state', how='inner')
    return {'unemployment_dataset': unemployment_df, 'crime_dataset': crime_df}

        

def load_data_to_db(df, db_name, table_name):
    """
    Loads DataFrame into an SQLite database.
    """
    engine = create_engine(f'sqlite:///{db_name}')
    conn = engine.connect()
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    print(f"Data successfully saved to {db_name}.")


def main():

    data = extract_data(UNEMPLOYMENT_DATA_SOURCE, CRIME_DATA_SOURCE)
    data = transform_data(data)
    unemployment_df = data['unemployment_dataset']
    crime_df = data['crime_dataset']
    
    load_data_to_db(unemployment_df, DB, TABLE1)
    load_data_to_db(crime_df, DB, TABLE2)
    
    
    
if __name__ == "__main__":
    main()
