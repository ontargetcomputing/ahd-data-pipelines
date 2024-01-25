from ahd_data_pipelines.integrations.object_datasource import ObjectDatasource
from pyspark.sql import SparkSession


class FileDatasource(ObjectDatasource):
    """
    """

    def __init__(self, params: dict = None, dbutils=None, spark: SparkSession = None, stage: str = 'DEV'):
        super(FileDatasource, self).__init__(params=params, dbutils=dbutils, spark=spark)

    def get_object(self):
        raise NotImplementedError('"file" datasource has not implemented get_object yet')
