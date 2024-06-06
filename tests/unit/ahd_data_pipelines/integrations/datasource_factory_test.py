import unittest
from unittest.mock import MagicMock
from unittest.mock import patch
from ahd_data_pipelines.integrations.datasource_factory import DatasourceFactory, DatasourceType
from ahd_data_pipelines.integrations.agol_datasource import AgolDatasource
from ahd_data_pipelines.integrations.s3_datasource import S3Datasource
from ahd_data_pipelines.integrations.db_datasource import DatabricksDatasource
from ahd_data_pipelines.integrations.http_datasource import HTTPDatasource
from ahd_data_pipelines.integrations.file_datasource import FileDatasource
from ahd_data_pipelines.integrations.noop_datasource import NoopDatasource
from ahd_data_pipelines.integrations.jdbc_datasource import JdbcDatasource
from ahd_data_pipelines.integrations.pandas_datasource import PandasDatasource
from pyspark.sql import SparkSession

class TestDatasourceFactory(unittest.TestCase):
    
    def setUp(self):
        self.mock_dbutils = MagicMock()
        self.mock_spark = MagicMock(spec=SparkSession)
    
    @patch('ahd_data_pipelines.integrations.datasource_factory.AgolDatasource', autospec=True)
    def test_get_agol_datasource(self, mock_agol_datasource):
        params = {"url": "https://example.com", "param2": "value2"}  # Add the 'url' key
        datasource = DatasourceFactory.getDatasource(DatasourceType.AGOL, params, dbutils=self.mock_dbutils, spark=self.mock_spark)
        self.assertIsInstance(datasource, AgolDatasource)

    def test_get_s3_datasource(self):
        params = {"param1": "value1", "param2": "value2", "agency": "CHHS"}
        datasource = DatasourceFactory.getDatasource(DatasourceType.S3, params, dbutils=self.mock_dbutils, spark=self.mock_spark)
        self.assertIsInstance(datasource, S3Datasource)

    def test_get_databricks_datasource(self):
        params = {"param1": "value1", "param2": "value2"}
        datasource = DatasourceFactory.getDatasource(DatasourceType.DATABRICKS, params, spark=self.mock_spark)
        self.assertIsInstance(datasource, DatabricksDatasource)

    def test_get_http_datasource(self):
        params = {"http_processor": "processor_name", "param2": "value2"}  # Add the 'http_processor' key
        datasource = DatasourceFactory.getDatasource(DatasourceType.HTTP, params, dbutils=self.mock_dbutils, spark=self.mock_spark)
        self.assertIsInstance(datasource, HTTPDatasource)

    def test_get_file_datasource(self):
        params = {"param1": "value1", "param2": "value2"}
        datasource = DatasourceFactory.getDatasource(DatasourceType.FILE, params, dbutils=self.mock_dbutils, spark=self.mock_spark)
        self.assertIsInstance(datasource, FileDatasource)

    def test_get_noop_datasource(self):
        datasource = DatasourceFactory.getDatasource(DatasourceType.NOOP, spark=self.mock_spark)
        self.assertIsInstance(datasource, NoopDatasource)

    def test_get_jdbc_datasource(self):
        params = {"host": "jdbc_host", "port": 5432, "driver": "postgresql", "database": "jdbc_database", "param2": "value2"}  # Assuming PostgreSQL JDBC connection
        datasource = DatasourceFactory.getDatasource(DatasourceType.JDBC, params, dbutils=self.mock_dbutils, spark=self.mock_spark)
        self.assertIsInstance(datasource, JdbcDatasource)

    def test_get_pandas_datasource(self):
        params = {"param1": "value1", "param2": "value2"}
        datasource = DatasourceFactory.getDatasource(DatasourceType.PANDAS, params, dbutils=self.mock_dbutils, spark=self.mock_spark)
        self.assertIsInstance(datasource, PandasDatasource)

if __name__ == '__main__':
    unittest.main()
