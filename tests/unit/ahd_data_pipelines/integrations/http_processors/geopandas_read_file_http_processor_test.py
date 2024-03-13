import unittest
from unittest.mock import patch, MagicMock
from ahd_data_pipelines.integrations.http_processors.geopandas_read_file_http_processor import GeoPandasReadFileHTTPProcessor

class TestGeoPandasReadFileHTTPProcessor(unittest.TestCase):

    @patch('ahd_data_pipelines.integrations.http_processors.geopandas_read_file_http_processor.gpd')
    @patch('ahd_data_pipelines.integrations.http_processors.geopandas_read_file_http_processor.PandasHelper')
    def test_process(self, mock_pandas_helper, mock_gpd):
        # Mocking the parameters and objects
        params = {'endpoint': 'http://example.com/shapefile.shp'}
        spark_mock = MagicMock()

        # Mocking the returned GeoDataFrame
        mock_gpd.read_file.return_value = MagicMock()
        
        # Mocking the PandasHelper method
        mock_pandas_helper.geopandas_to_pysparksql.return_value = MagicMock()

        # Initializing the processor
        processor = GeoPandasReadFileHTTPProcessor()

        # Running the process method
        processor.process(params, spark_mock)

        # Assertions
        mock_gpd.read_file.assert_called_once_with('http://example.com/shapefile.shp')
        mock_pandas_helper.geopandas_to_pysparksql.assert_called_once()

if __name__ == '__main__':
    unittest.main()
