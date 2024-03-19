import unittest
from unittest.mock import MagicMock
from ahd_data_pipelines.transformations.drop_columns import DropColumns
import pyspark.sql as pyspark_sql
import geopandas as gpd
import pandas as pd


class TestDropColumnsTransformation(unittest.TestCase):
    def test_drop_columns_pyspark(self):
        # Prepare
        spark = MagicMock()
        spark_dataframe = MagicMock(spec=pyspark_sql.DataFrame)
        spark_dataframe.drop = MagicMock(return_value=spark_dataframe)
        params = {'drop_columns': ['col1', 'col2']}
        
        # Execute
        result = DropColumns.execute(dataFrame=spark_dataframe, params=params, spark=spark)
        
        # Assert
        self.assertEqual(spark_dataframe.drop.call_count, 2)
        spark_dataframe.drop.assert_any_call('col1')
        spark_dataframe.drop.assert_any_call('col2')
        self.assertEqual(result, spark_dataframe)
        
    def test_drop_columns_geopandas(self):
        # Prepare
        geodataframe = MagicMock(spec=gpd.GeoDataFrame)
        geodataframe.drop = MagicMock(return_value=geodataframe)
        params = {'drop_columns': ['col1', 'col2']}
        
        # Execute
        result = DropColumns.execute(dataFrame=geodataframe, params=params)
        
        # Assert
        geodataframe.drop.assert_called_once_with(columns=['col1', 'col2'], inplace=True)
        self.assertEqual(result, geodataframe)
        
    def test_drop_columns_pandas(self):
        # Prepare
        dataframe = MagicMock(spec=pd.DataFrame)
        params = {'drop_columns': ['col1', 'col2']}
        
        # Execute
        result = DropColumns.execute(dataFrame=dataframe, params=params)
        
        # Assert
        dataframe.drop.assert_called_once_with(columns=['col1', 'col2'], inplace=True)
        self.assertEqual(result, dataframe)


if __name__ == '__main__':
    unittest.main()
