import pandas as pd
import kagglehub
from sqlalchemy import create_engine
import os

from config import *


def fetch_csv(url):
    """
    Downloads CSV file from a given URL.
    """
    try:
        path = kagglehub.dataset_download(url)
        csv_file_path = path + f'/{os.listdir(path)[0]}'
        
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"CSV file not found at {csv_file_path}")
        
        df = pd.read_csv(csv_file_path)
        print("CSV file successfully loaded.")
        return df
    
    except Exception as e:
        print("Error loading CSV file: %s", str(e))
        raise

        
def clean_data(df):
    """
    Clean data and modify column names properly.
    """
    try:
        if df.isnull().values.any():

            # Drop missing values for simplicity
            df.dropna(inplace=True)
        
        # Standardize column names
        df.columns = df.columns.str.lower().str.replace(" (%) ", "_").str.replace("state/area", "state").str.replace(" ", "_")
        print(df.columns)
        
        return df
    
    except Exception as e:
        print("Error in cleaning the data: %s", str(e))
        raise


def extract_data(unemployment_data_source, crime_data_source):
    """
    Extract data from sources.
    """
    try:
        unemployment_df = fetch_csv(unemployment_data_source)
        crime_df = fetch_csv(crime_data_source)
        
        return {'unemployment_dataset': unemployment_df, 'crime_dataset': crime_df}
    except Exception as e:
        print("Error in data extraction: %s", str(e))
        raise


def transform_data(data_dict):
    """
    Transform data by aggregating datasets.
    """
    try:
        unemployment_df = clean_data(data_dict['unemployment_dataset'])
        crime_df = clean_data(data_dict['crime_dataset'])
        
        month_mapping = {
        "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6,
        "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12
        }
        crime_df["month"] = crime_df["month"].map(month_mapping)

        # Aggregate the us_crime data by state, year, and month
        crime_summary = crime_df.groupby(["state", "year", "month"]).agg({
            "incident": "sum",         # Total incidents
            "victim_count": "sum",     # Total victims
            "perpetrator_count": "sum" # Total perpetrators
        }).reset_index()

        # Merge with unemployment data frame
        merged_df = pd.merge(
            crime_summary,
            unemployment_df,
            on=["state", "year", "month"],
            how="inner"
        )
        for col in merged_df.columns:
            if 'total' in col:
                merged_df[col] = merged_df[col].str.replace(',', '').astype('int64')


        # merged_df.to_csv("aggregated_crime_unemployment.csv", index=False)


        # print(unemployment_df.shape, crime_df.shape)
        # data = pd.merge(unemployment_df, crime_df, on='state', how='inner')
        return {'unemployment_dataset': unemployment_df, 'crime_dataset': crime_df, "merged": merged_df}
    
    except Exception as e:
        print("Error in data transformation: %s", str(e))
        raise

        

def load_data_to_db(df, db_name, table_name):
    """
    Loads DataFrame into an SQLite database.
    """
    try:
        engine = create_engine(f'sqlite:///{db_name}')
        conn = engine.connect()
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        print(f"Data successfully saved to {db_name}.")

    except Exception as e:
        print("Error in data loading to database: %s", str(e))
        raise


def main():

    data = extract_data(UNEMPLOYMENT_DATA_SOURCE, CRIME_DATA_SOURCE)
    data = transform_data(data)
    unemployment_df = data['unemployment_dataset']
    crime_df = data['crime_dataset']
    merged_df = data['merged']

    merged_df.head().to_csv('./header.csv')

    load_data_to_db(merged_df, DB, 'US_crime_unemployment')

    
if __name__ == "__main__":
    main()
