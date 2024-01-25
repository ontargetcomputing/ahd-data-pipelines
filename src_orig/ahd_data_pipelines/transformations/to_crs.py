from cdii_data_pipelines.transformations.transformation import Transformation
from cdii_data_pipelines.pandas.pandas_helper import PandasHelper


class ToCRS(Transformation):
    def to_perform(params):
        return ('to_crs' in params.keys())

    def execute(dataFrame, params: dict = None, spark=None):
        geo_df = PandasHelper.pysparksql_to_geopandas(dataFrame)

        column = params['to_crs']['column']
        to_crs = params['to_crs']['to_crs']
        from_crs = params['to_crs']['from_crs']
        print(f'Transforming {column} from {from_crs} to {to_crs}')

        geo_df = geo_df.set_crs(from_crs)
        geo_df = geo_df.to_crs(to_crs)

        return PandasHelper.geopandas_to_pysparksql(geo_df, spark=spark)
