from ahd_data_pipelines.integrations.datasource import Datasource
from pyspark.sql import SparkSession
from pyspark.pandas import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    DateType,
    TimestampType,
)


class PandasDatasource(Datasource):
    """ """

    def __init__(self, params: dict = None, spark: SparkSession = None):
        self.params = params
        self.spark = spark

    def read(self) -> DataFrame:
        print(f"Reading {self.params}")

        structs = []
        dtypes = self.params["dtypes"]
        for dtype in dtypes:
            print(f"Adding dtype: {dtype}")
            key = list(dtype.keys())[0]
            value = list(dtype.values())[0]
            if value == "string":
                structs.append(StructField(key, StringType()))
            elif value == "int":
                structs.append(StructField(key, IntegerType()))
            elif value == "double":
                structs.append(StructField(key, DoubleType()))
            elif value == "date":
                structs.append(StructField(key, DateType()))
            elif value == "timestamp":
                structs.append(StructField(key, TimestampType()))
            else:
                raise TypeError(f"Unknown type {value}")

        schema = StructType(structs)

        df = self.spark.createDataFrame(self.params["data"], schema=schema)
        print(f"Found {df.count()} records")

        return df

    def write(self, dataFrame: DataFrame):
        raise NotImplementedError("'write' is not implemented")

    def truncate(self):
        raise NotImplementedError("'truncate' is not implemented")
