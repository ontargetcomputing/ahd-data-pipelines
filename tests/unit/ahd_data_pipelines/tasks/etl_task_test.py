from ahd_data_pipelines.tasks.etl_task import ETLTask
from pyspark.sql import SparkSession
from pyspark.pandas import DataFrame


class ConcreteETLTask(ETLTask):
    def __init__(self, spark: SparkSession=None, init_conf: dict=None):
      super(ConcreteETLTask, self).__init__(spark=spark, init_conf=init_conf)

    def transform(self, dataFrame: DataFrame,  params: dict=None) -> DataFrame:
      return None

def test_default_source_datasource_is_empty():
    concreteETLTask = ConcreteETLTask()
    assert len(concreteETLTask.sources) == 0

def test_default_destination_datasource_is_empty():
    concreteETLTask = ConcreteETLTask()
    assert len(concreteETLTask.destinations) == 0

def test_source_datasource_list_of_1():
    params = {
        "source_datasources": [
          {
          "type": "noop",
          }
        ]
    }
    concreteETLTask = ConcreteETLTask(init_conf=params)
    assert len(concreteETLTask.sources) == 1

def test_destination_datasource_list_of_1():
    params = {
        "destination_datasources": [
          {
          "type": "noop",
          }
        ]
    }
    concreteETLTask = ConcreteETLTask(init_conf=params)
    assert len(concreteETLTask.destinations) == 1

def test_source_datasource_list_of_more_than_1():
    params = {
        "source_datasources": [
          {
          "type": "noop",
          },
          {
          "type": "noop",
          }
        ]
    }
    concreteETLTask = ConcreteETLTask(init_conf=params)
    assert len(concreteETLTask.sources) == 2

def test_destination_datasource_list_of_more_than_1():
    params = {
        "destination_datasources": [
          {
          "type": "noop",
          },
          {
          "type": "noop",
          }
        ]
    }
    concreteETLTask = ConcreteETLTask(init_conf=params)
    assert len(concreteETLTask.destinations) == 2