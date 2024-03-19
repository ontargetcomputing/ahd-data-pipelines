from ahd_data_pipelines.integrations.s3_datasource import S3Datasource
from pyspark.sql import SparkSession
import pytest
import os

spark = SparkSession.builder.getOrCreate()


def test_truncate_raises_notimplementederror():
    params = {
        'format': 'excel',
        'aws_access_key': 'aaaa',
        'aws_secret_key': 'bbbb',
        'aws_region': 'us-west-1',
        'bucket_name': 'aaaa',
        'agency': 'agency',
        'file_name': 'file'
    }
    with pytest.raises(NotImplementedError) as theerror:
       s3Datasource = S3Datasource(params=params, spark=spark)
       s3Datasource.truncate()
    
    assert str(theerror.value).startswith("'truncate' is not implemented") is True

@pytest.mark.skip(reason="this is not a unit test, but let's add to CI later.  Need to setup test file")
def test_read_works():
    aws_access_key = os.getenv('AWS_ACCESS_KEY')
    aws_secret_key = os.getenv('AWS_SECRET_KEY')
    params = {
        'format': 'excel',
        'aws_access_key': aws_access_key,
        'aws_secret_key': aws_secret_key,
        'aws_region': 'us-west-1',
        'bucket_name': 'chhs-datahub-emsa',
        'agency': 'EMSA',
        'file_name': 'upload/Shared with HHS ESRI.xlsx'
    }
   
    s3Datasource = S3Datasource(params=params, spark=spark)
    dataframe = s3Datasource.read()
    
    assert dataframe.count() > 0
   



