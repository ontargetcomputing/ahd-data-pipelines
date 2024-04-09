from ahd_data_pipelines.integrations.jdbc_datasource import JdbcDatasource
from pyspark.sql import SparkSession
import pytest
import os

spark = SparkSession.builder.getOrCreate()

def test_write_raises_notimplementederror():
    params = {

    }
    with pytest.raises(NotImplementedError) as theerror:
       jdbcDatasource = JdbcDatasource(params=params, spark=spark)
       jdbcDatasource.write(dataFrame=None)
    
    assert str(theerror.value).startswith("'write' is not implemented") is True

def test_truncate_raises_notimplementederror():
    params = {

    }
    with pytest.raises(NotImplementedError) as theerror:
       jdbcDatasource = JdbcDatasource(params=params, spark=spark)
       jdbcDatasource.truncate()
    
    assert str(theerror.value).startswith("'truncate' is not implemented") is True

