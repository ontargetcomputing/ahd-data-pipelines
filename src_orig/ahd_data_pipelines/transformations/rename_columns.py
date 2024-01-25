from cdii_data_pipelines.transformations.transformation import Transformation
import pyspark as pyspark
import geopandas as gpd
import pandas as pd
import os


class RenameColumns(Transformation):
    def to_perform(params):
        return (('rename_columns' in params.keys()) and (len(params['rename_columns']) > 0))

    def execute(dataFrame, params: dict = None, spark=None):
        rename_columns = params['rename_columns']
        print(f'Renaming columns {rename_columns}')

        if os.environ.get('LOCAL') == 'true':
            if isinstance(dataFrame, pyspark.sql.connect.dataframe.DataFrame):
              for rename_column in rename_columns:
                  nvp = rename_column.rsplit(':', 1)
                  dataFrame = dataFrame.withColumnRenamed(nvp[0], nvp[1])
        elif isinstance(dataFrame, pyspark.sql.DataFrame): 
            print('renaming columns in pyspark.sql.DataFrame')
            for rename_column in rename_columns:
                nvp = rename_column.rsplit(':', 1)
                dataFrame = dataFrame.withColumnRenamed(nvp[0], nvp[1])
        elif isinstance(dataFrame, gpd.GeoDataFrame) or isinstance(dataFrame, pd.DataFrame):
            print('renaming columns in GeoDataFrame or DataFrame')
            for rename_column in rename_columns:
                nvp = rename_column.rsplit(':', 1)
                dataFrame = dataFrame.rename(columns={nvp[0]: nvp[1]})

        return dataFrame
