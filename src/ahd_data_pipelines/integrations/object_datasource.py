from ahd_data_pipelines.integrations.datasource import Datasource
from pyspark.sql import SparkSession
from pyspark.pandas import DataFrame
import json
import pandas as pd
import io
from ahd_data_pipelines.pandas.pandas_helper import PandasHelper
from openpyxl.styles import Alignment, Border, Side, Font
from openpyxl import Workbook


class ObjectDatasource(Datasource):
    """
    """

    def __init__(self, params: dict = None, dbutils=None, spark: SparkSession = None):
        if params is None:
            raise TypeError("NoneType not allowed for params")

        self.params = params
        self.spark = spark
        self.dbutils = dbutils

    def get_object(self) -> str:
        if self.params.get('text') is None:
            raise TypeError("NoneType not allowed for params['text']")
        return self.params['text']

    def read(self) -> DataFrame:
        object = self.get_object()

        if isinstance(object, type(self.spark.createDataFrame([()]))):
            print('get_object returned a DataFrame, returning it')
            return object
        else:
            format = self.params.get('format')
            print(f'get_object did not return a DataFrame, processing as {format}')

            if format == 'csv':
                raise NotImplementedError('csv has not been implemented as a text format yet.')
            elif format == 'yaml':
                raise NotImplementedError('yaml has not been implemented as a text format yet.')
            elif format == 'json':
                return self._create_dataframe_from_json_text(object)
            elif format == 'json_object':
                return self._create_dataframe_from_json_object(object)
            elif format == 'excel':  # yes, this is a hack, this is not text but let's go with it.
                return self._create_dataframe_from_excel(object)
            else:
                raise ValueError(f"Unknown object format '{format}'")

    def _create_dataframe_from_json_object(self, json_object):
        print(json_object)
        print(type(json_object))
        if len(json_object) == 0:
            dataframe = pd.DataFrame()
        else:
            dataframe = pd.DataFrame(json_object)

        return PandasHelper.pandas_to_pysparksql(pd_df=dataframe, spark=self.spark)

    def _create_dataframe_from_json_text(self, json_text):
        # for now, _create_dataframe_from_json is expected an array of dicts.
        # we should make this configurable in future.
        json_object = json.loads(json_text)
        return self._create_dataframe_from_json_object(json_object)

    def _create_dataframe_from_excel(self, excel_object):
        dataFrame = pd.read_excel(io.BytesIO(excel_object), engine="openpyxl")
        dataFrame = dataFrame.fillna('')
        dataFrame = dataFrame.astype(str)
        return PandasHelper.pandas_to_pysparksql(pd_df=dataFrame, spark=self.spark)

    def write(self, dataFrame: DataFrame):
        format = self.params.get('format')
        print(f'writing as {format}')

        if format == 'csv':
            raise NotImplementedError('csv has not been implemented as a text format yet.')
        elif format == 'yaml':
            raise NotImplementedError('yaml has not been implemented as a text format yet.')
        elif format == 'json':
            raise NotImplementedError('json has not been implemented as a text format yet.')
        elif format == 'json_object':
            raise NotImplementedError('json_object has not been implemented as a text format yet.')
        elif format == 'excel':  # yes, this is a hack, this is not text but let's go with it.
            obj = self._get_excel(dataFrame)
            self._write(obj)
        else:
            raise ValueError(f"Unknown object format '{format}'")

    def _get_excel(self, dataFrame: DataFrame):
        pandas_df = dataFrame.toPandas()

        workbook = Workbook()
        worksheet = workbook.active
        for index, row in pandas_df.iterrows():
            worksheet.append(row.tolist())

        num_rows, num_cols = pandas_df.shape
        border_style = Border(left=Side(style='thin'),
                              right=Side(style='thin'),
                              top=Side(style='thin'),
                              bottom=Side(style='thin'))

        for row in range(2, num_rows + 2):
            for col in range(1, num_cols + 1):
                cell = worksheet.cell(row=row, column=col)
                cell.alignment = Alignment(horizontal='center')
                cell.border = border_style
                cell.font = Font(size=12, name='Calibri')

        return workbook

    def _write(object):
        pass
