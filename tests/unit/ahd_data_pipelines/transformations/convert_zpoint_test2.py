import unittest
from pyspark.sql import SparkSession
from shapely import wkt
from ahd_data_pipelines.transformations.convert_zpoint import ConvertZPoint

class TestConvertZPoint(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize Spark session
        cls.spark = SparkSession.builder \
            .master("local[2]") \
            .appName("TestConvertZPoint") \
            .getOrCreate()
        
    @classmethod
    def tearDownClass(cls):
        # Stop Spark session
        cls.spark.stop()

    def test_execute(self):
        # Sample data
        data = [(1, 2, "POINT (2.0 1.0)"), (3, 4, "POINT (4.0 3.0)")]
        columns = ['x', 'y', 'geometry']
        
        # Create Spark DataFrame
        df = self.spark.createDataFrame(data, columns)
        
        # Parameters
        params = {'convert_zpoint': 'geometry'}
        
        # Execute the transformation
        result_df = ConvertZPoint.execute(df, params=params, spark=self.spark)
        
        # Check if result DataFrame is a Spark DataFrame
        self.assertTrue(result_df.__class__.__name__ == 'DataFrame')

        # Check if 'geometry' column is present
        self.assertIn('geometry', result_df.columns)

        # Get the resulting geometries
        geometries = result_df.select('geometry').rdd.flatMap(lambda x: x).collect()

        # Check if the geometries are correctly computed
        expected_geometries = [wkt.loads("POINT (2.0 1.0)"), wkt.loads("POINT (4.0 3.0)")]
        for expected_geometry, computed_geometry_str in zip(expected_geometries, geometries):
            computed_geometry = wkt.loads(computed_geometry_str)
            self.assertEqual(expected_geometry, computed_geometry)

if __name__ == '__main__':
    unittest.main()
