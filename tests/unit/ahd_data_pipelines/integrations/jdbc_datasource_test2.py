import unittest
from unittest.mock import MagicMock, patch
from ahd_data_pipelines.integrations.datasource import Datasource
from ahd_data_pipelines.integrations.jdbc_datasource import JdbcDatasource  # Replace 'my_module' with your module name
from pyspark.sql import SparkSession
from pyspark.pandas import DataFrame
import pandas as pd
import sqlalchemy as sq

class TestJdbcDatasource(unittest.TestCase):

    def setUp(self):
        self.params = {
            'driver': 'my_driver',
            'username': 'my_username',
            'password': 'my_password',
            'host': 'my_host',
            'port': 'my_port',
            'database': 'my_database',
            'query': 'my_query'
        }
        self.spark = SparkSession.builder.master("local").appName("test").getOrCreate()

    def tearDown(self):
        self.spark.stop()

    @patch('ahd_data_pipelines.integrations.jdbc_datasource.sq.create_engine')
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
        self.assertIsInstance(result, DataFrame)
        self.assertEqual(len(result), 0)

if __name__ == '__main__':
    unittest.main()
