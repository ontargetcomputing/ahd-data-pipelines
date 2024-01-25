from ahd_data_pipelines.integrations.datasource import Datasource
from ahd_data_pipelines.integrations.agol_datasource import AgolDatasource
from ahd_data_pipelines.integrations.db_datasource import DatabricksDatasource
from ahd_data_pipelines.integrations.noop_datasource import NoopDatasource
from ahd_data_pipelines.integrations.datasource_type import DatasourceType
from ahd_data_pipelines.integrations.s3_datasource import S3Datasource
from ahd_data_pipelines.integrations.http_datasource import HTTPDatasource
from ahd_data_pipelines.integrations.file_datasource import FileDatasource
from ahd_data_pipelines.integrations.jdbc_datasource import JdbcDatasource
from ahd_data_pipelines.integrations.pandas_datasource import PandasDatasource
import os
from pyspark.sql import SparkSession


class DatasourceFactory:
    @staticmethod
    def getDatasource(type: DatasourceType = DatasourceType.DATABRICKS, params: dict = None,
                      dbutils=None, spark: SparkSession = None, stage: str = 'DEV') -> Datasource:
        print(f'Constructing Datasource : ${params}')
        print(f'Dbutils is {dbutils}')
        if type == DatasourceType.AGOL:
            return DatasourceFactory.getAgolDatasource(params=params, dbutils=dbutils, spark=spark, stage=stage)
        elif type == DatasourceType.DATABRICKS:
            return DatasourceFactory.getDatabricksDatasource(params=params, spark=spark)
        elif type == DatasourceType.NOOP:
            return DatasourceFactory.getNoopDatasource(spark=spark)
        elif type == DatasourceType.S3:
            return DatasourceFactory.getS3Datasource(params=params, dbutils=dbutils, spark=spark, stage=stage)
        elif type == DatasourceType.HTTP:
            return DatasourceFactory.getHTTPDatasource(params=params, dbutils=dbutils, spark=spark, stage=stage)
        elif type == DatasourceType.FILE:
            return DatasourceFactory.getFileDatasource(params=params, dbutils=dbutils, spark=spark, stage=stage)
        elif type == DatasourceType.JDBC:
            return DatasourceFactory.getJdbcDatasource(params=params, dbutils=dbutils, spark=spark, stage=stage)
        elif type == DatasourceType.PANDAS:
            return DatasourceFactory.getPandasDatasource(params=params, dbutils=dbutils, spark=spark, stage=stage)
        else:
            raise ValueError(type)

    @staticmethod
    def getAgolDatasource(params: dict = None, dbutils=None, spark: SparkSession = None,
                          stage: str = 'DEV') -> AgolDatasource:
        if dbutils is None or os.environ.get('LOCAL') == 'true':
            print('Reading AGOL CREDS from environment')
            agol_user = os.environ.get(f'AGOL_USERNAME_{stage}')
            agol_password = os.environ.get(f'AGOL_PASSWORD_{stage}')
        else:
            print('Reading AGOL CREDS from dbutils')
            agol_user = dbutils.secrets.get("SECRET_KEYS", f'AGOL_USERNAME_{stage}')
            agol_password = dbutils.secrets.get("SECRET_KEYS", f'AGOL_PASSWORD_{stage}')

        params['username'] = agol_user
        params['password'] = agol_password
        return AgolDatasource(params=params, spark=spark)

    @staticmethod
    def getS3Datasource(params: dict = None, dbutils=None, spark: SparkSession = None,
                        stage: str = 'DEV') -> S3Datasource:
        agency = params.get('agency', 'CHHS')
        if dbutils is None or os.environ.get('LOCAL') == 'true':
            print(f'Reading AWS CREDS from environment for agency:{agency}')
            aws_access_key = os.environ.get(f'AWS_ACCESS_KEY_{agency}_{stage}')
            aws_secret_key = os.environ.get(f'AWS_SECRET_KEY_{agency}_{stage}')
        else:
            print(f'Reading AWS CREDS from dbutils for agency:{agency}')
            aws_access_key = dbutils.secrets.get("SECRET_KEYS", f'AWS_ACCESS_KEY_{agency}_{stage}')
            aws_secret_key = dbutils.secrets.get("SECRET_KEYS", f'AWS_SECRET_KEY_{agency}_{stage}')

        params['aws_access_key'] = aws_access_key
        params['aws_secret_key'] = aws_secret_key
        return S3Datasource(params=params, dbutils=dbutils, spark=spark, stage=stage)

    @staticmethod
    def getDatabricksDatasource(params: dict = None, dbutils=None, spark: SparkSession = None, stage: str = 'DEV'):
        return DatabricksDatasource(params=params, spark=spark)

    @staticmethod
    def getHTTPDatasource(params: dict = None, dbutils=None, spark: SparkSession = None, stage: str = 'DEV'):
        return HTTPDatasource(params=params, spark=spark)

    @staticmethod
    def getFileDatasource(params: dict = None, dbutils=None, spark: SparkSession = None, stage: str = 'DEV'):
        return FileDatasource(params=params, dbutils=dbutils, spark=spark)

    @staticmethod
    def getNoopDatasource(spark: SparkSession = None, stage: str = 'DEV'):
        return NoopDatasource(spark=spark)

    @staticmethod
    def getJdbcDatasource(params: dict = None, dbutils=None, spark: SparkSession = None,
                          stage: str = 'DEV') -> AgolDatasource:
        if dbutils is None or os.environ.get('LOCAL') == 'true':
            print('Reading jdbc credentials from environment')
            jdbc_user = os.environ.get(f'JDBC_USERNAME_{stage}')
            jdbc_password = os.environ.get(f'JDBC_PASSWORD_{stage}')
        else:
            print('Reading jdbc credentials from dbutils')
            jdbc_user = dbutils.secrets.get("SECRET_KEYS", f'JDBC_USERNAME_{stage}')
            jdbc_password = dbutils.secrets.get("SECRET_KEYS", f'JDBC_PASSWORD_{stage}')

        params['username'] = jdbc_user
        params['password'] = jdbc_password
        return JdbcDatasource(params=params, spark=spark)

    @staticmethod
    def getPandasDatasource(params: dict = None, dbutils=None, spark: SparkSession = None,
                            stage: str = 'DEV') -> AgolDatasource:
        return PandasDatasource(params=params, spark=spark)
