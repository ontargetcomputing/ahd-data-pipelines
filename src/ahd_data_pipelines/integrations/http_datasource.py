from ahd_data_pipelines.integrations.object_datasource import ObjectDatasource
from pyspark.sql import SparkSession
import importlib


class HTTPDatasource(ObjectDatasource):
    """
    """

    def __init__(self, params: dict = None, spark: SparkSession = None, stage: str = 'DEV'):
        super(HTTPDatasource, self).__init__(params=params, spark=spark)
        self.http_processor = params['http_processor']

    def get_object(self):
        # Import the module dynamically
        package_name, module_name = self.http_processor.rsplit('.', 1)
        module = importlib.import_module(package_name)
        clazz = getattr(module, module_name)
        instance = clazz()
        return instance.process(self.params, spark=self.spark)
