from cdii_data_pipelines.integrations.object_datasource import ObjectDatasource
from pyspark.sql import SparkSession
import boto3
from io import BytesIO


class S3Datasource(ObjectDatasource):
    """
    """

    def __init__(self, params: dict = None, dbutils=None, spark: SparkSession = None, stage: str = 'DEV'):
        super(S3Datasource, self).__init__(params=params)
        aws_access_key = params['aws_access_key']
        aws_secret_key = params['aws_secret_key']
        region_name = params.get('aws_region', 'us-west-2')
        self.s3 = boto3.resource(
            service_name='s3',
            region_name=region_name,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key)
        self.params = params
        self.spark = spark

    def get_object(self):
        bucket_name = self.params['bucket_name']
        file_name = self.params['file_name']
        bucket = self.s3.Bucket(bucket_name)
        obj = bucket.Object(file_name).get()
        data = obj['Body'].read()
        return data

    def _write(self, obj):
        bucket_name = self.params['bucket_name']
        file_name = self.params['file_name']
        output = BytesIO()
        obj.save(output)

        self.s3.Object(bucket_name, file_name).put(Body=output.getvalue())
