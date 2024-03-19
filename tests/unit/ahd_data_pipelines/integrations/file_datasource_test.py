import unittest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from ahd_data_pipelines.integrations.file_datasource import FileDatasource

class TestFileDatasource(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("unittest") \
            .getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_get_object_not_implemented(self):
        # Arrange
        params = {"some_param": "some_value"}
        datasource = FileDatasource(params=params, spark=self.spark)

        # Act & Assert
        with self.assertRaises(NotImplementedError):
            datasource.get_object()

if __name__ == '__main__':
    unittest.main()
