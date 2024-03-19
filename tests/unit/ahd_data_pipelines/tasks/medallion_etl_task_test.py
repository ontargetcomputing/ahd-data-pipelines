# import unittest
# from pyspark.sql import SparkSession
# from unittest.mock import patch, MagicMock
# from ahd_data_pipelines.tasks.medallion_etl_task import MedallionETLTask

# class TestMedallionETLTask(unittest.TestCase):

#     @classmethod
#     def setUpClass(cls):
#         # Create a SparkSession for testing
#         cls.spark = SparkSession.builder \
#             .appName("TestMedallionETLTask") \
#             .master("local[2]") \
#             .getOrCreate()

#     @classmethod
#     def tearDownClass(cls):
#         # Stop the SparkSession
#         cls.spark.stop()

#     @patch('delta.tables.DeltaTable')
#     def test_transform_bronze_false(self, mock_delta_table):
#         # Mocking DeltaTable
#         mock_delta_table.return_value = MagicMock()

#         # Create an instance of MedallionETLTask with bronze=False
#         task = MedallionETLTask(spark=self.spark, init_conf={'bronze': False})

#         # Create some sample data
#         data = [(1, "Alice", 30), (2, "Bob", 25)]
#         df = self.spark.createDataFrame(data, ["id", "name", "age"])

#         # Call the transform method
#         result = task.transform(dataFrames=[df], params={})

#         # Assert that custom_transform is called and returns the correct DataFrame
#         self.assertEqual(result, [df])

#         # Additional assertions can be added here to verify transformations or other logic

#     @patch('delta.tables.DeltaTable')
#     def test_transform_bronze_true(self, mock_delta_table):
#         # Mocking DeltaTable
#         mock_delta_table.return_value = MagicMock()

#         # Create an instance of MedallionETLTask with bronze=True
#         task = MedallionETLTask(spark=self.spark, init_conf={'bronze': True})

#         # Create some sample data
#         data = [(1, "Alice", 30), (2, "Bob", 25)]
#         df = self.spark.createDataFrame(data, ["id", "name", "age"])

#         # Call the transform method
#         result = task.transform(dataFrames=[df], params={})

#         # Assert that __outbound_transform is called and returns the correct DataFrame
#         self.assertEqual(result, [df])

#         # Additional assertions can be added here to verify transformations or other logic

# if __name__ == '__main__':
#     unittest.main()
