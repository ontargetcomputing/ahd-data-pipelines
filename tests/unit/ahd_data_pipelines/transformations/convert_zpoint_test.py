import unittest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from ahd_data_pipelines.transformations.convert_zpoint import ConvertZPoint

class TestConvertZPoint(unittest.TestCase):

    def test_convert_zpoint(self):
        # Create a SparkSession
        spark = SparkSession.builder \
            .appName("ConvertZPointTest") \
            .getOrCreate()

        # Create a sample GeoDataFrame
        data = {'ID': [1, 2, 3],
                'geometry': [Point(1, 2), Point(3, 4), Point(5, 6)]}
        gdf = gpd.GeoDataFrame(data, crs='EPSG:4326')

        # Convert geometry to WKT
        gdf['geometry'] = gdf['geometry'].apply(lambda geom: geom.wkt)

        # Define schema for Spark DataFrame
        schema = StructType([
            StructField("ID", IntegerType(), True),
            StructField("geometry", StringType(), True)
        ])

        # Create Spark DataFrame with schema
        spark_df = spark.createDataFrame(gdf, schema=schema)

        # Set up parameters
        params = {'convert_zpoint': 'geometry'}

        # Perform conversion
        converted_df = ConvertZPoint.execute(spark_df, params=params, spark=spark)

        # Check if the conversion was successful
        self.assertTrue('latitude' not in converted_df.columns)
        self.assertTrue('longitude' not in converted_df.columns)
        self.assertTrue('geometry' in converted_df.columns)
        self.assertEqual(converted_df.count(), 3)  # Make sure number of rows is preserved

        # Stop SparkSession
        spark.stop()

if __name__ == '__main__':
    unittest.main()
