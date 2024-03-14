from ahd_data_pipelines.integrations.datasource import Datasource
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
#from delta.tables import DeltaTable
from ahd_data_pipelines.pandas.pandas_helper import PandasHelper
import os

class DatabricksDatasource(Datasource):
    """
    """

    def __init__(self, params: dict = None, spark: SparkSession = None):
        self.params = params
        self.spark = spark

    def read(self) -> DataFrame:
        table_name = self.params.get('table', None)
        query = self.params.get('query', None)
        if table_name is not None:
            print(f'Reading from {table_name}')
            try:
                dataframe = self.spark.read.table(table_name)
            except AnalysisException as ae:
                if os.environ.get('LOCAL') == 'true':
                  if ae.message.startswith('[TABLE_OR_VIEW_NOT_FOUND]'):
                      dataframe = PandasHelper.empty_spark_sql_dataframe(self.spark)
                  else:
                      raise
                else:
                  if ae.desc.startswith('[TABLE_OR_VIEW_NOT_FOUND]'):
                      dataframe = PandasHelper.empty_spark_sql_dataframe(self.spark)
                  else:
                      raise
        elif query is not None:
            print(f'Performing "{query}"')
            try:
                dataframe = self.spark.sql(query)
            except AnalysisException as ae:
                if os.environ.get('LOCAL') == 'true':
                  if ae.message.startswith('[TABLE_OR_VIEW_NOT_FOUND]'):
                      dataframe = PandasHelper.empty_spark_sql_dataframe(self.spark)
                  else:
                      raise
                else:
                  if ae.desc.startswith('[TABLE_OR_VIEW_NOT_FOUND]'):
                      dataframe = PandasHelper.empty_spark_sql_dataframe(self.spark)
                  else:
                      raise
        else:
            raise ValueError("Please provide either 'table' or 'query' to datasource")

        if len(dataframe.schema) <= 0:
            print('********   Table has 0 columns or does not exist   ************')
            dataframe = PandasHelper.empty_spark_sql_dataframe(self.spark)

        return dataframe

    def write(self, dataFrame: DataFrame):
        table_name = self.params['table']

        if dataFrame.count() > 0:
            DATA_TYPES = 'data_types'
            if DATA_TYPES in self.params.keys():
                data_types = self.params[DATA_TYPES]
                for data_type in data_types:
                    column = data_type['column']
                    type = data_type['type']
                    if column in dataFrame.columns:
                        print(f'Casting {column} to {type}')
                        dataFrame = dataFrame.withColumn(column, dataFrame[column].cast(type))
                    else:
                        print(f'{column} does not exist.')
            else:
                print('No datatypes to cast')

            method = 'overwrite'
            if 'method' in self.params.keys():
                method = self.params['method']

            if method == 'overwrite' or method == 'append':
                print(f'{method} table with {dataFrame.count()} records')
                dataFrame.write.mode(method).format("delta").option("mergeSchema", "true").saveAsTable(table_name)
            elif method == 'merge':
                if dataFrame.count() > 0:
                    record_count = dataFrame.count()
                    print(f'There are {record_count} records to merge in')
                    
                    target_df = self.spark.getActiveSession().table(table_name)
                    merge_on = self.params['merge_on']

                    merge_expr = dataFrame[merge_on] == target_df[merge_on]
                    
                    merged_df = target_df.join(dataFrame, merge_expr, "inner") \
                                        .drop(target_df[merge_on]) \
                                        .withColumnRenamed(merge_on, f"source_{merge_on}") \
                                        .dropDuplicates()
                    
                    merged_df.write.mode("overwrite").saveAsTable(table_name)
                else:
                    print('No data in source, nothing to merge')

            else:
                raise ValueError(f'Unknown method {method}')

    def truncate(self):
        table_name = self.params['table']
        try:
          self.spark.sql(f'truncate table {table_name}')
        except AnalysisException as ae:
          msg = '********   Table has 0 columns or does not exist, nothing to truncate   ************'
          ## HACK!! spark is different local vs db ##
          if os.environ.get('LOCAL') == 'true':
            if ae.message.startswith('[TABLE_OR_VIEW_NOT_FOUND]'):
                print(msg)
            else:
                raise
          else:
            if ae.desc.startswith('[TABLE_OR_VIEW_NOT_FOUND]'):
                print(msg)
            else:
                raise


