from cdii_data_pipelines.integrations.datasource import Datasource
from pyspark.sql import SparkSession
from pyspark.pandas import DataFrame
import pandas as pd
import sqlalchemy as sq
import geopandas as gpd
from cdii_data_pipelines.pandas.pandas_helper import PandasHelper
from shapely import wkt


class JdbcDatasource(Datasource):
    """
    """

    def __init__(self, params: dict = None, spark: SparkSession = None):
        self.params = params
        self.spark = spark

        jdbc_url = f'''{params['driver']}://{params['username']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}''' # noqa: E501, E261
        self.connection = sq.create_engine(jdbc_url)

    def read(self) -> DataFrame:
        postgis = self.params.get('postgis', None)
        if postgis is None:
            dataFrame = pd.read_sql_query(self.params['query'], self.connection)
        else:
            column = postgis.get('column', 'geometry')
            dataFrame = gpd.read_postgis(sql=self.params['query'],
                                         con=self.connection,
                                         geom_col=column,
                                         crs=postgis.get('crs', 4326))
            dataFrame[column] = dataFrame[column].apply(wkt.dumps)

        if len(dataFrame) > 0:
            return PandasHelper.to_pysparksql(pd_df=dataFrame, spark=self.spark)
        else:
            return PandasHelper.empty_spark_sql_dataframe(spark=self.spark)

    def write(self, dataFrame: DataFrame):
        raise NotImplementedError("'write' is not implemented")

    def truncate(self):
        raise NotImplementedError("'truncate' is not implemented")
