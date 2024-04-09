from ahd_data_pipelines.integrations.pandas_datasource import PandasDatasource
from pyspark.sql import SparkSession
import pytest
import os

spark = SparkSession.builder.getOrCreate()


def test_read():
    params = {
        'data': [
            {"name": "John", "age": 30},
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 35}
        ]
    }

    pandasDatasource = PandasDatasource(params=params, spark=spark)


    df = pandasDatasource.read()

    assert df.count() == 3
    assert df.first()['name'] == 'John'