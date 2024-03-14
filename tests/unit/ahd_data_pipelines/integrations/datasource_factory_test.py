import unittest
from unittest.mock import Mock
from ahd_data_pipelines.integrations.datasource_factory import DatasourceFactory, DatasourceType
from ahd_data_pipelines.integrations.agol_datasource import AgolDatasource
from ahd_data_pipelines.integrations.s3_datasource import S3Datasource
from ahd_data_pipelines.integrations.db_datasource import DatabricksDatasource
from pyspark.sql import SparkSession

class TestDatasourceFactory(unittest.TestCase):
    
    def setUp(self):
        self.mock_dbutils = Mock()

    # def test_get_datasource_agol(self):
    #     self.mock_spark = Mock(spec=SparkSession)
    #     self.mock_spark.return_value.__len__ = Mock(return_value=1)
    #     params = {'url': 'https://example.com/agol', 'param1': 'value1'}
    #     datasource = DatasourceFactory.getDatasource(type=DatasourceType.AGOL, params=params, dbutils=self.mock_dbutils, spark=self.mock_spark)
    #     self.assertIsInstance(datasource, AgolDatasource)
    
    def test_get_datasource_s3(self):
        self.mock_spark = Mock(spec=SparkSession)
        self.mock_spark.return_value.__len__ = Mock(return_value=1)
        params = {'param1': 'value1'}
        datasource = DatasourceFactory.getDatasource(type=DatasourceType.S3, params=params, dbutils=self.mock_dbutils, spark=self.mock_spark)
        self.assertIsInstance(datasource, S3Datasource)
    
    def test_get_datasource_databricks(self):
        self.mock_spark = Mock(spec=SparkSession)
        self.mock_spark.return_value.__len__ = Mock(return_value=1)
        params = {'param1': 'value1'}
        datasource = DatasourceFactory.getDatasource(type=DatasourceType.DATABRICKS, params=params, dbutils=self.mock_dbutils, spark=self.mock_spark)
        self.assertIsInstance(datasource, DatabricksDatasource)

if __name__ == '__main__':
    unittest.main()
