from ahd_data_pipelines.integrations.http_processors.http_processor import HTTPProcessor
from ahd_data_pipelines.pandas.pandas_helper import PandasHelper
import geopandas as gpd


class GeoPandasReadFileHTTPProcessor(HTTPProcessor):
    """ """

    def process(self, params, spark=None):
        endpoint = params["endpoint"]
        gp_df = gpd.read_file(endpoint)
        return PandasHelper.geopandas_to_pysparksql(gpd_df=gp_df, spark=spark)
