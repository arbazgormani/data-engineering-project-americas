import pandas as pd
import kagglehub
from sqlalchemy import create_engine

from config import *


def read_csv(url):
    """
    Downloads CSV file from a given URL.
    """
    if 'kaggle' in url:
        path = kagglehub.dataset_download("mrayushagrawal/us-crime-dataset")

        print("Path to dataset files:", path)
        csv_file_path = path + '/US_Crime_DataSet.csv'  # Replace with your CSV file's name
        df = pd.read_csv(csv_file_path)
    else:
        df = pd.read_csv(url, encoding="utf-16")
    return df
        

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

    # df = read_csv(DATA_SOURCE1)
    # print(df)
    load_data_to_db(read_csv(DATA_SOURCE1), DB, TABLE1)
    load_data_to_db(read_csv(DATA_SOURCE2), DB, TABLE2)
    
    
if __name__ == "__main__":
    main()
