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

    @patch('jdbc_datasource.gpd.read_postgis')
    @patch('jdbc_datasource.PandasHelper')
    @patch('pyspark.sql.DataFrame')
    @patch('sqlalchemy.create_engine')
    def test_read_with_postgis(self, mock_pandas_helper, mock_read_postgis):
        # Mocking parameters and SparkSession
        params = {
            "driver": "mock_driver",
            "username": "mock_username",
            "password": "mock_password",
            "host": "mock_host",
            "port": "mock_port",
            "database": "mock_database",
            "query": "SELECT * FROM mock_table",
            "postgis": {
                "column": "mock_geometry_column",
                "crs": 4326
            }
        }
        spark_session_mock = MagicMock(spec=SparkSession)

        # Mocking Datasource instance
        datasource = MagicMock(spec=Datasource)
        datasource.params = params
        datasource.spark = spark_session_mock

        # Mocking read_postgis return value
        mock_data_frame = MagicMock(spec=gpd.GeoDataFrame)
        mock_read_postgis.return_value = mock_data_frame

        # Mocking PandasHelper.to_pysparksql return value
        mock_spark_data_frame = MagicMock(spec=DataFrame)
        mock_pandas_helper.to_pysparksql.return_value = mock_spark_data_frame

        # Creating JdbcDatasource instance and calling read method
        jdbc_datasource = JdbcDatasource(params=params, spark=spark_session_mock)
        result = jdbc_datasource.read()

        # Assertions
        mock_read_postgis.assert_called_once_with(
            sql=params["query"],
            con=sq.create_engine.return_value,
            geom_col=params["postgis"]["column"],
            crs=params["postgis"]["crs"]
        )
        mock_pandas_helper.to_pysparksql.assert_called_once_with(pd_df=mock_data_frame, spark=spark_session_mock)
        self.assertEqual(result, mock_spark_data_frame)

    @patch('sqlalchemy.create_engine')
    @patch('pyspark.sql.DataFrame')
    def test_write_not_implemented(self, *args):
        jdbc_datasource = JdbcDatasource(params=self.params, spark=self.spark)
        with self.assertRaises(NotImplementedError):
            jdbc_datasource.write(DataFrame())

    @patch('sqlalchemy.create_engine')
    @patch('pyspark.sql.DataFrame')
    def test_truncate_not_implemented(self, *args):
        jdbc_datasource = JdbcDatasource(params=self.params, spark=self.spark)
        with self.assertRaises(NotImplementedError):
            jdbc_datasource.truncate()

if __name__ == '__main__':
    unittest.main()
