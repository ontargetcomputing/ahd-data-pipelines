from ahd_data_pipelines.transformations.transformation import Transformation
from ahd_data_pipelines.pandas.pandas_helper import PandasHelper
import geopandas as gpd


class ConvertZPoint(Transformation):
    def to_perform(params):
        return "convert_zpoint" in params.keys()

    def execute(dataFrame, params: dict = None, spark=None):
        geo_df = PandasHelper.pysparksql_to_geopandas(dataFrame)

        column = params['convert_zpoint']
        print(f'Converting ZPoint on {column}')
        geo_df['latitude'] = geo_df[column].y
        geo_df['longitude'] = geo_df[column].x
        geo_df.drop(['geometry'], axis=1, inplace=True)
        geo_df['geometry'] = gpd.points_from_xy(geo_df.longitude, geo_df.latitude)
        geo_df.drop(['longitude'], axis=1, inplace=True)
        geo_df.drop(['latitude'], axis=1, inplace=True)

        return PandasHelper.geopandas_to_pysparksql(geo_df, spark=spark)
