from cdii_data_pipelines.transformations.transformation import Transformation
import pyspark as pyspark
import geopandas as gpd
import pandas as pd


class DropColumns(Transformation):
    def to_perform(params):
        return 'drop_columns' in params.keys()

    def execute(dataFrame, params: dict = None, spark=None):
        cols_to_drop = params['drop_columns']
        if isinstance(dataFrame, pyspark.sql.DataFrame):
            print('dropping columns in pyspark.sql.DataFrame')
            for column in cols_to_drop:
                print(f'Dropping column - {column}')
                dataFrame = dataFrame.drop(column)
        elif isinstance(dataFrame, gpd.GeoDataFrame) or isinstance(dataFrame, pd.DataFrame):
            print('dropping columns in GeoDataFrame or DataFrame')
            dataFrame.drop(columns=cols_to_drop, inplace=True)

        return dataFrame
