from ahd_data_pipelines.integrations.http_datasource import HTTPDatasource
from pyspark.sql import SparkSession
import pytest
import os

spark = SparkSession.builder.getOrCreate()

@pytest.mark.skip(reason="Don't test this, just for use locally for now")
def test_something():
    params = {
        'format': 'json',
        'processor': 'ahd_data_pipelines.integrations.http_processors.fema_http_processor',
        'classname': 'HTTPProcessor'
    }

    http_datasource = HTTPDatasource(params=params)
    http_datasource.read()
