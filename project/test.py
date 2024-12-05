import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pipeline import fetch_csv, clean_data, extract_data, transform_data, load_data_to_db
from sqlalchemy import create_engine


class EmptyDatabaseException(Exception):
    pass


class TestPipeline(unittest.TestCase):
    """
    Test cases for the pipeline.
    """

    @patch("pipeline.kagglehub.dataset_download")
    @patch("pipeline.os.listdir")
    @patch("os.path.exists")
    @patch("pandas.read_csv")
    def test_fetch_csv(self, mock_read_csv, mock_exists, mock_listdir, mock_dataset_download):
        """
        test fetch csv function.
        """
        mock_dataset_download.return_value = "/mocked/dataset/path"
        mock_listdir.return_value = ["mock_file.csv"]
        mock_exists.return_value = True

        mock_data = {
            "state": ["Wyoming", "Wisconsin", "West Virginia", "Washington", "Virginia"],
            "year": [1980, 1990, 1982, 1994, 1998],
            "month": [1, 2, 4, 6, 8],
            "incident": [1, 3, 3, 2, 1],
            "victim_count": [56, 56, 56, 56, 56],
            "perpetrator_count": [329000, 330000, 331000, 333000, 334000]
        }
        mock_read_csv.return_value = pd.DataFrame(mock_data)

        df = fetch_csv("kaggle.mock_url.dataset")

        mock_dataset_download.assert_called_once_with(
            "kaggle.mock_url.dataset")
        mock_read_csv.assert_called_once_with(
            "/mocked/dataset/path/mock_file.csv")

    @patch("pandas.read_csv")
    def test_clean_data(self, mock_read_csv):
        """
        test clean data function.
        """
        mock_data = {
            "State/Area": ["Wyoming", "Wisconsin", "West Virginia", "Washington", "Virginia"],
            "Year": [1980, 1982, 1994, None, 1998],
            "Month": [1, 2, 4, 6, 8],
            "Incident": [1, 3, 3, 2, 1],
            "Victim Count": [56, 56, 56, 56, 56],
            "Perpetrator Count": [329000, 330000, 331000, 334000, None],
            "Total civilian non-institutional Population In State": [329000, 330000, 331000, 333000, 334000],
            "percent (%) of State's Population": [70.2, 70.2, 70.2, 69.9, 69.9],
        }

        df = pd.DataFrame(mock_data)

        cleaned_df = clean_data(df)
        # dropping NA values
        self.assertEqual(df.shape, (3, 8))

        # Standardize column names
        self.assertTrue("state" in cleaned_df.columns)
        self.assertTrue("year" in cleaned_df.columns)
        self.assertTrue("month" in cleaned_df.columns)
        self.assertTrue("incident" in cleaned_df.columns)
        self.assertTrue("victim_count" in cleaned_df.columns)
        self.assertTrue("perpetrator_count" in cleaned_df.columns)
        self.assertTrue(
            "total_civilian_non-institutional_population_in_state" in cleaned_df.columns)
        self.assertTrue("percent_of_state's_population" in cleaned_df.columns)

    @patch("pipeline.fetch_csv")
    def test_extract_data(self, mock_fetch_csv):
        """
        test extract from ETL pipeline.
        """
        mock_unemployment_dataset = pd.DataFrame({
            "state": ["Wyoming", "Wisconsin", "West Virginia", "Washington", "Virginia"],
            "year": [1980, 1990, 1982, 1994, 1998],
            "month": [1, 2, 4, 6, 8],
            "fips_code": [1, 2, 4, 4, 5],
            "total_civilian_non-institutional_population_in_state": [329000, 330000, 331000, 333000, 334000],
            "total_civilian_labor_force_in_state": [329000, 330000, 331000, 333000, 334000],
            "percent_of_state's_population": [70.2, 70.2, 70.2, 69.9, 69.9],
            "total_employment_in_state": [223835, 224309, 224572, 224749, 224983],
            "percent_of_labor_force_employed_in_state": [70.2, 70.2, 70.2, 69.9, 69.9],
            "total_unemployment_in_state": [223835, 224309, 224572, 224749, 224983],
            "percent_of_labor_force_unemployed_in_state": [70.2, 70.2, 70.2, 69.9, 69.9]
        })

        mock_crime_dataset = pd.DataFrame({
            "state": ["Wyoming", "Wisconsin", "West Virginia", "Washington", "Virginia"],
            "year": [1980, 1990, 1982, 1994, 1998],
            "month": [1, 2, 4, 6, 8],
            "incident": [1, 3, 3, 2, 1],
            "victim_count": [56, 56, 56, 56, 56],
            "perpetrator_count": [329000, 330000, 331000, 333000, 334000],
        })
        mock_fetch_csv.side_effect = [
            mock_unemployment_dataset, mock_crime_dataset]
        result = extract_data(
            "mock.kaggle.unemployment_url.dataset", "mock.kaggle.crime_url.dataset")

        self.assertIn("unemployment_dataset", result)
        assert result['unemployment_dataset'].equals(mock_unemployment_dataset)
        self.assertIn("crime_dataset", result)
        assert result['crime_dataset'].equals(mock_crime_dataset)

    @patch("pipeline.clean_data")
    def test_transform_data(self, mock_clean_data):
        """
        test transform from ETL pipeline.
        """
        data_dict = {
            'unemployment_dataset': pd.DataFrame({
                "state": ["Wyoming", "Wisconsin", "West Virginia", "Washington", "Virginia"],
                "year": [1990, 1990, 1982, 1994, 1998],
                "month": [1, 2, 4, 6, 8],
                "fips_code": [1, 2, 4, 4, 5],
                "total_civilian_labor_force_in_state": ['3,29,000', '3,30,000', '3,31,000', '3,33,000', '3,34,000'],
                "percent_of_state's_population": [70.2, 70.2, 70.2, 69.9, 69.9],
                "percent_of_labor_force_employed_in_state": [70.2, 70.2, 70.2, 69.9, 69.9],
                "total_unemployment_in_state": ['2,23,835', '2,24,309', '2,24,572', '2,24,749', '2,24,983'],
                "percent_of_labor_force_unemployed_in_state": [70.2, 70.2, 70.2, 69.9, 69.9]
            }),
            'crime_dataset': pd.DataFrame({
                "state": ["Wyoming", "Wyoming", "West Virginia", "Washington", "Virginia"],
                "year": [1990, 1990, 1982, 1994, 1998],
                "month": ["January", "January", "April", "May", "April"],
                "incident": [1, 3, 3, 2, 1],
                "victim_count": [56, 56, 56, 56, 56],
                "perpetrator_count": [3290, 3300, 3310, 3330, 3400],
            })
        }
        mock_clean_data.side_effect = [
            data_dict["unemployment_dataset"], data_dict["crime_dataset"]]
        mock_transformed_data = pd.DataFrame({
            'state': ['West Virginia', 'Wyoming'],
            'year': [1982, 1990], 'month': [4, 1],
            'incident': [3, 4], 'victim_count': [56, 112],
            'perpetrator_count': [3310, 6590], 'fips_code': [4, 1],
            'total_civilian_labor_force_in_state': [331000, 329000],
            "percent_of_state's_population": [70.2, 70.2],
            'percent_of_labor_force_employed_in_state': [70.2, 70.2],
            'total_unemployment_in_state': [224572, 223835],
            'percent_of_labor_force_unemployed_in_state': [70.2, 70.2]
        }
        )

        result = transform_data(data_dict)
        self.assertIn("merged", result)
        self.assertEqual(result["merged"].shape, (2, 12))
        assert result['merged'].equals(mock_transformed_data)

    @patch("pipeline.create_engine")
    def test_load_data_to_db(self, mock_create_engine):
        """
        test Load from ETL pipeline.
        """
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_transformed_data = pd.DataFrame({
            'state': ['West Virginia', 'Wyoming'],
            'year': [1982, 1990], 'month': [4, 1],
            'incident': [3, 4], 'victim_count': [56, 112],
            'perpetrator_count': [3310, 6590], 'fips_code': [4, 1],
            'total_civilian_labor_force_in_state': [331000, 329000],
            "percent_of_state's_population": [70.2, 70.2],
            'percent_of_labor_force_employed_in_state': [70.2, 70.2],
            'total_unemployment_in_state': [224572, 223835],
            'percent_of_labor_force_unemployed_in_state': [70.2, 70.2]
        }
        )

        load_data_to_db(mock_transformed_data,
                        "mock_test_db.sqlite", "test_table")

        mock_create_engine.assert_called_once_with(
            'sqlite:///mock_test_db.sqlite')
        mock_engine.connect.assert_called_once()

    def test_db_content(self):
        """
        test database content from load ETL pipeline.
        """
        engine = create_engine(f'sqlite:///{'./data/project_americas.db'}')
        connection = engine.connect()
        try:
            df = pd.read_sql(
                'select * from US_crime_unemployment;', connection)

            if df.empty:
                print("No data found for the query.")
                raise EmptyDatabaseException("No data found in the database!!")
        except Exception as e:
            print(f"Error executing query: {e}")

        connection.close()
        self.assertEqual(df.shape[1], 14)


if __name__ == "__main__":
    unittest.main()
