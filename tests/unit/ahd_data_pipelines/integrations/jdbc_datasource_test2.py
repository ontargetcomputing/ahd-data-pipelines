import unittest
from unittest.mock import patch, MagicMock
from ahd_data_pipelines.integrations.datasource import Datasource
from ahd_data_pipelines.integrations.jdbc_datasource import JdbcDatasource
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.pandas import DataFrame
import pandas as pd

class TestJdbcDatasource(unittest.TestCase):

    def setUp(self):
        self.params = {
            'driver': 'my_driver',
            'username': 'my_username',
            'password': 'my_password',
            'host': 'my_host',
            'port': 9001,
            'database': 'my_database',
            'query': 'my_query'
        }
        self.spark = SparkSession.builder.master("local").appName("test").getOrCreate()

    def tearDown(self):
        self.spark.stop()

    @patch('sqlalchemy.create_engine')
    def test_read_empty_dataframe(self, mock_create_engine):
        # Mocking out create_engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.execute.return_value = pd.DataFrame()  # Return an empty DataFrame
        # Instantiate JdbcDatasource
        jdbc_datasource = JdbcDatasource(params=self.params, spark=self.spark)
        # Call read method
        result = jdbc_datasource.read()
        # Assertions
        self.assertIsInstance(result, SparkDataFrame)
        self.assertEqual(result.count(), 0)

    @patch('sqlalchemy.create_engine')
    @patch('pyspark.sql.DataFrame')
    def test_read_with_postgis_none(self, mock_dataframe, mock_create_engine):
        # Mocking out create_engine
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        # Set postgis to None
        self.params['postgis'] = None
        # Instantiate JdbcDatasource with postgis as None
        jdbc_datasource = JdbcDatasource(params=self.params, spark=self.spark)
        # Call read method
        result = jdbc_datasource.read()
        # Assertions
        self.assertIsInstance(result, SparkDataFrame)
        self.assertEqual(result.count(), 0)

if __name__ == '__main__':
    unittest.main()
