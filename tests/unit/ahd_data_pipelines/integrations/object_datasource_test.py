from ahd_data_pipelines.integrations.object_datasource import ObjectDatasource
from pyspark.sql import SparkSession
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


spark = SparkSession.builder.getOrCreate()

def test_read_raises_valueerror_when_no_params():
    with pytest.raises(TypeError) as theerror:
        object_datasource = ObjectDatasource(spark=spark)
        object_datasource.read()

    assert str(theerror.value) == "NoneType not allowed for params"

def test_read_raises_valueerror_when_no_text_in_params():
    with pytest.raises(TypeError) as theerror:
        object_datasource = ObjectDatasource(params = { 
          'format': 'json'
          }, spark=spark)       
        object_datasource.read()

    assert str(theerror.value) == "NoneType not allowed for params['text']"

def test_write_raises_notimplementederror():
    with pytest.raises(NotImplementedError) as theerror:
        object_datasource = ObjectDatasource(params = { 
          'text': '{"name": "John", "age": 30, "city": "New York"}',
          'format': 'json'
          }, spark=spark)
        object_datasource.write(dataFrame=None)

    assert str(theerror.value).startswith("json has not been implemented as a text format yet.") is True

def test_truncate_raises_notimplementederror():
    with pytest.raises(NotImplementedError) as theerror:
        object_datasource = ObjectDatasource(params = { 
          'text': '{"name": "John", "age": 30, "city": "New York"}',
          'format': 'json'
          }, spark=spark)
        object_datasource.truncate()
    
    assert str(theerror.value).startswith("'truncate' is not implemented") is True

def test_read_raises_notimplementederror_when_no_format():
    with pytest.raises(ValueError) as theerror:
        object_datasource = ObjectDatasource(params = { 'text': 'foobar'}, spark=spark)
        object_datasource.read()
    
    assert str(theerror.value) == "Unknown object format 'None'"

def test_read_works_on_json():
    data = {'Name': ['Tom', 'nick', 'krish', 'jack'],
        'Age': [20, 21, 19, 18]}
    object_datasource = ObjectDatasource(params = { 
        'text': data,
        'format': 'json_object'
        }, spark=spark)

    dataframe = object_datasource.read()

    assert dataframe.count() == 4
    assert dataframe.select("name").first()[0] == "Tom"
    assert dataframe.select("age").first()[0] == 20

def test_read_raises_notimplementederror_when_csv():
    with pytest.raises(NotImplementedError) as theerror:
        object_datasource = ObjectDatasource(params = { 
            'text': 'foobar',
            'format': 'csv'
          },
          spark=spark)
        object_datasource.read()
    
    assert str(theerror.value) == 'csv has not been implemented as a text format yet.'

def test_read_raises_notimplementederror_when_yaml():
    with pytest.raises(NotImplementedError) as theerror:
        object_datasource = ObjectDatasource(params = { 
            'text': 'foobar',
            'format': 'yaml'
          },
          spark=spark)
        object_datasource.read()
    
    assert str(theerror.value) == 'yaml has not been implemented as a text format yet.'

def test_read_returns_dataframe_when_received():
    class SubObjectDatasource(ObjectDatasource):
        def get_object(self):
            schema = StructType([
                StructField("name", StringType(), nullable=False),
                StructField("age", IntegerType(), nullable=False)
            ])

            # Create a DataFrame with a single record
            data = [("John", 25)]
            return spark.createDataFrame(data, schema)
        
    subObjectDatasource = SubObjectDatasource(params = {}, spark = spark)
    dataframe = subObjectDatasource.read()

    assert dataframe.count() == 1
    assert dataframe.select("name").first()[0] == "John"
    assert dataframe.select("age").first()[0] == 25

