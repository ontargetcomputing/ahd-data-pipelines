import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, Polygon
from ahd_data_pipelines.pandas.pandas_helper import PandasHelper
from ahd_data_pipelines.integrations.agol_datasource import AgolDatasource  # Adjust the import according to your actual module name

class TestAgolDatasource(unittest.TestCase):

    @patch('ahd_data_pipelines.integrations.agol_datasource.GIS')
    @patch('ahd_data_pipelines.integrations.agol_datasource.Table')
    @patch('ahd_data_pipelines.pandas.pandas_helper.PandasHelper.pandas_to_pysparksql')
    def test_read_from_table(self, mock_pandas_to_pysparksql, mock_Table, mock_GIS):
        mock_gis_instance = MagicMock()
        mock_GIS.return_value = mock_gis_instance

        mock_table_instance = mock_Table.return_value
        mock_table_instance.query.return_value.features = [
            MagicMock(attributes={'field1': 'value1', 'field2': 'value2'}),
            MagicMock(attributes={'field1': 'value3', 'field2': 'value4'}),
        ]

        mock_spark_df = MagicMock()
        mock_pandas_to_pysparksql.return_value = mock_spark_df

        spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

        params = {
            'url': 'http://fake-url',
            'username': 'fake-user',
            'password': 'fake-pass',
            'dataset_id': 'fake-dataset-id',
            'table_index': 0,
            'is_table': True
        }

        datasource = AgolDatasource(params=params, spark=spark)

        result = datasource.read_from_table()

        mock_GIS.assert_called_once_with('http://fake-url', 'fake-user', 'fake-pass')
        mock_gis_instance.content.get.assert_called_once_with('fake-dataset-id')
        mock_Table.assert_called_once_with(mock_gis_instance.content.get.return_value.tables[0].url)
        mock_table_instance.query.assert_called_once()

        expected_data = pd.DataFrame([
            {'field1': 'value1', 'field2': 'value2'},
            {'field1': 'value3', 'field2': 'value4'}
        ])

        pd.testing.assert_frame_equal(
            mock_pandas_to_pysparksql.call_args[0][0],
            expected_data
        )


    @patch('ahd_data_pipelines.integrations.agol_datasource.GIS')
    @patch('ahd_data_pipelines.integrations.agol_datasource.FeatureLayer')
    @patch('ahd_data_pipelines.pandas.pandas_helper.PandasHelper.geopandas_to_pysparksql')
    def test_read_from_feature_layer(self, mock_geopandas_to_pysparksql, mock_FeatureLayer, mock_GIS):
        mock_gis_instance = MagicMock()
        mock_GIS.return_value = mock_gis_instance

        mock_feature_layer_instance = mock_FeatureLayer.return_value
        mock_gis_instance.content.get.return_value.layers = [mock_feature_layer_instance]
        mock_feature_layer_instance.url = "fake-layer-url"
        
        featureSet = MagicMock()
        featureSet.sdf = pd.DataFrame({
            'geometry': [Point(1, 2), Polygon([(0, 0), (1, 1), (1, 0)])]
        })
        # Ensure to_geojson returns a valid JSON string
        featureSet.to_geojson.return_value = '{"features": [{"geometry": {"type": "Point", "coordinates": [1, 2]}, "properties": {"field1": "value1"}},{"geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 1], [1, 0], [0, 0]]]}, "properties": {"field2": "value2"}}]}'
        
        mock_feature_layer_instance.query.return_value = featureSet

        mock_spark_df = MagicMock()
        mock_geopandas_to_pysparksql.return_value = mock_spark_df

        spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

        params = {
            'url': 'http://fake-url',
            'username': 'fake-user',
            'password': 'fake-pass',
            'dataset_id': 'fake-dataset-id',
            'layer': 0,
            'is_table': False,
            'original_crs': 3857,
            'new_crs': 4326
        }

        datasource = AgolDatasource(params=params, spark=spark)

        result = datasource.read_from_feature_layer()

        mock_GIS.assert_called_once_with('http://fake-url', 'fake-user', 'fake-pass')
        mock_gis_instance.content.get.assert_called_once_with('fake-dataset-id')
        mock_feature_layer_instance.query.assert_called_once()

        expected_gdf = gpd.GeoDataFrame({
            'field1': ['value1', None],
            'field2': [None, 'value2'],
            'geometry': [Point(1, 2), Polygon([(0, 0), (1, 1), (1, 0), (0, 0)])]
        }, geometry='geometry', crs="EPSG:4326")

        gpd.testing.assert_geodataframe_equal(
            mock_geopandas_to_pysparksql.call_args[0][0],
            expected_gdf
        )

        self.assertEqual(result, mock_spark_df)
        spark.stop()




    @patch('ahd_data_pipelines.integrations.agol_datasource.GIS')
    @patch('ahd_data_pipelines.integrations.agol_datasource.FeatureLayer')
    def test_write_to_feature_layer(self, mock_FeatureLayer, mock_GIS):
        mock_gis_instance = MagicMock()
        mock_GIS.return_value = mock_gis_instance

        mock_feature_layer_instance = mock_FeatureLayer.return_value

        spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

        params = {
            'url': 'http://fake-url',
            'username': 'fake-user',
            'password': 'fake-pass',
            'dataset_id': 'fake-dataset-id',
            'layer': 0,
            'geometry': {'column': 'geometry', 'type': 'POINT'},
            'is_table': False
        }

        datasource = AgolDatasource(params=params, spark=spark)

        data = pd.DataFrame({
            'field1': ['value1', 'value2'],
            'geometry': ['POINT (1 2)', 'POINT (3 4)']
        })
        data['geometry'] = data['geometry'].apply(wkt.loads)
        spark_df = PandasHelper.pandas_to_pysparksql(pd_df=data, spark=spark)

        with patch.object(datasource, 'truncate_feature_layer') as mock_truncate:
            datasource.write_to_feature_layer(spark_df)
            mock_truncate.assert_called_once()

        mock_GIS.assert_called_once_with('http://fake-url', 'fake-user', 'fake-pass')
        mock_gis_instance.content.get.assert_called_once_with('fake-dataset-id')
        mock_FeatureLayer.assert_called_once_with(mock_gis_instance.content.get.return_value.layers[0].url)
        
        expected_features = [
            {
                "attributes": {"field1": "value1"},
                "geometry": {"x": 1, "y": 2, "spatialReference": {"wkid": 4326}}
            },
            {
                "attributes": {"field1": "value2"},
                "geometry": {"x": 3, "y": 4, "spatialReference": {"wkid": 4326}}
            }
        ]
        mock_feature_layer_instance.edit_features.assert_called_once_with(adds=expected_features)
        spark.stop()

    @patch('ahd_data_pipelines.integrations.agol_datasource.GIS')
    @patch('ahd_data_pipelines.integrations.agol_datasource.Table')
    def test_write_to_table(self, mock_Table, mock_GIS):
        mock_gis_instance = MagicMock()
        mock_GIS.return_value = mock_gis_instance

        mock_table_instance = mock_Table.return_value

        spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

        params = {
            'url': 'http://fake-url',
            'username': 'fake-user',
            'password': 'fake-pass',
            'dataset_id': 'fake-dataset-id',
            'table_index': 0,
            'is_table': True
        }

        datasource = AgolDatasource(params=params, spark=spark)

        data = pd.DataFrame({
            'field1': ['value1', 'value2'],
            'field2': [None, 'value4']
        })
        spark_df = PandasHelper.pandas_to_pysparksql(pd_df=data, spark=spark)

        with patch.object(datasource, 'truncate_table') as mock_truncate:
            datasource.write_to_table(spark_df)
            mock_truncate.assert_called_once()

        mock_GIS.assert_called_once_with('http://fake-url', 'fake-user', 'fake-pass')
        mock_gis_instance.content.get.assert_called_once_with('fake-dataset-id')
        mock_Table.assert_called_once_with(mock_gis_instance.content.get.return_value.tables[0].url)
        
        expected_rows = [
            {"attributes": {"field1": "value1", "field2": None}},
            {"attributes": {"field1": "value2", "field2": "value4"}}
        ]
        mock_table_instance.edit_features.assert_called_once_with(adds=expected_rows)
        spark.stop()

    @patch('ahd_data_pipelines.integrations.agol_datasource.GIS')
    @patch('ahd_data_pipelines.integrations.agol_datasource.FeatureLayer')
    def test_truncate_feature_layer(self, mock_FeatureLayer, mock_GIS):
        mock_gis_instance = MagicMock()
        mock_GIS.return_value = mock_gis_instance

        mock_feature_layer_instance = mock_FeatureLayer.return_value

        spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

        params = {
            'url': 'http://fake-url',
            'username': 'fake-user',
            'password': 'fake-pass',
            'dataset_id': 'fake-dataset-id',
            'layer': 0,
            'object_id': 'OBJECTID',
            'is_table': False
        }

        datasource = AgolDatasource(params=params, spark=spark)

        datasource.truncate_feature_layer()

        mock_GIS.assert_called_once_with('http://fake-url', 'fake-user', 'fake-pass')
        mock_gis_instance.content.get.assert_called_once_with('fake-dataset-id')
        mock_FeatureLayer.assert_called_once_with(mock_gis_instance.content.get.return_value.layers[0].url)
        mock_feature_layer_instance.delete_features.assert_called_once_with(where="OBJECTID > 0")
        spark.stop()

    @patch('ahd_data_pipelines.integrations.agol_datasource.GIS')
    @patch('ahd_data_pipelines.integrations.agol_datasource.Table')
    def test_truncate_table(self, mock_Table, mock_GIS):
        mock_gis_instance = MagicMock()
        mock_GIS.return_value = mock_gis_instance

        mock_table_instance = mock_Table.return_value

        spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

        params = {
            'url': 'http://fake-url',
            'username': 'fake-user',
            'password': 'fake-pass',
            'dataset_id': 'fake-dataset-id',
            'table_index': 0,
            'object_id': 'OBJECTID',
            'is_table': True
        }

        datasource = AgolDatasource(params=params, spark=spark)

        datasource.truncate_table()

        mock_GIS.assert_called_once_with('http://fake-url', 'fake-user', 'fake-pass')
        mock_gis_instance.content.get.assert_called_once_with('fake-dataset-id')
        mock_Table.assert_called_once_with(mock_gis_instance.content.get.return_value.tables[0].url)
        mock_table_instance.delete_features.assert_called_once_with(where="OBJECTID > 0")
        spark.stop()

    @patch('ahd_data_pipelines.integrations.agol_datasource.GIS')
    def test_read(self, mock_GIS):
        spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

        params = {
            'url': 'http://fake-url',
            'username': 'fake-user',
            'password': 'fake-pass',
            'dataset_id': 'fake-dataset-id',
            'is_table': True,
            'table_index': 0
        }

        datasource = AgolDatasource(params=params, spark=spark)

        with patch.object(datasource, 'read_from_table') as mock_read_table, \
             patch.object(datasource, 'read_from_feature_layer') as mock_read_feature_layer:
            datasource.read()
            mock_read_table.assert_called_once()
            mock_read_feature_layer.assert_not_called()

        params['is_table'] = False

        datasource = AgolDatasource(params=params, spark=spark)

        with patch.object(datasource, 'read_from_table') as mock_read_table, \
             patch.object(datasource, 'read_from_feature_layer') as mock_read_feature_layer:
            datasource.read()
            mock_read_table.assert_not_called()
            mock_read_feature_layer.assert_called_once()
        spark.stop()

    @patch('ahd_data_pipelines.integrations.agol_datasource.GIS')
    def test_write(self, mock_GIS):
        spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

        params = {
            'url': 'http://fake-url',
            'username': 'fake-user',
            'password': 'fake-pass',
            'dataset_id': 'fake-dataset-id',
            'is_table': True,
            'table_index': 0
        }

        datasource = AgolDatasource(params=params, spark=spark)

        data = pd.DataFrame({
            'field1': ['value1', 'value2'],
            'field2': [None, 'value4']
        })
        spark_df = PandasHelper.pandas_to_pysparksql(pd_df=data, spark=spark)

        with patch.object(datasource, 'write_to_table') as mock_write_table, \
             patch.object(datasource, 'write_to_feature_layer') as mock_write_feature_layer:
            datasource.write(spark_df)
            mock_write_table.assert_called_once_with(spark_df)
            mock_write_feature_layer.assert_not_called()

        params['is_table'] = False

        datasource = AgolDatasource(params=params, spark=spark)

        with patch.object(datasource, 'write_to_table') as mock_write_table, \
             patch.object(datasource, 'write_to_feature_layer') as mock_write_feature_layer:
            datasource.write(spark_df)
            mock_write_table.assert_not_called()
            mock_write_feature_layer.assert_called_once_with(spark_df)
        spark.stop()

    @patch('ahd_data_pipelines.integrations.agol_datasource.GIS')
    def test_truncate(self, mock_GIS):
        spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

        params = {
            'url': 'http://fake-url',
            'username': 'fake-user',
            'password': 'fake-pass',
            'dataset_id': 'fake-dataset-id',
            'is_table': True,
            'table_index': 0
        }

        datasource = AgolDatasource(params=params, spark=spark)

        with patch.object(datasource, 'truncate_table') as mock_truncate_table, \
             patch.object(datasource, 'truncate_feature_layer') as mock_truncate_feature_layer:
            datasource.truncate()
            mock_truncate_table.assert_called_once()
            mock_truncate_feature_layer.assert_not_called()

        params['is_table'] = False

        datasource = AgolDatasource(params=params, spark=spark)

        with patch.object(datasource, 'truncate_table') as mock_truncate_table, \
             patch.object(datasource, 'truncate_feature_layer') as mock_truncate_feature_layer:
            datasource.truncate()
            mock_truncate_table.assert_not_called()
            mock_truncate_feature_layer.assert_called_once()
        spark.stop()


if __name__ == '__main__':
    unittest.main()
