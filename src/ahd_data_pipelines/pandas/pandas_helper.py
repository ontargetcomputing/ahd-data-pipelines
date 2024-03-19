import pyspark as pyspark
import geopandas as gpd
import pandas as pd
from shapely import wkt
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
import os


class PandasHelper:
    @staticmethod
    def pysparksql_to_pandas_or_geopandas(
        pysparksql_df: pyspark.sql.DataFrame = None,
    ) -> gpd.GeoDataFrame:
        if "true" != os.environ.get("LOCAL"):
            if not isinstance(pysparksql_df, pyspark.sql.DataFrame):
                raise ValueError(f"Unexpected type;{type(pysparksql_df)}")
        else:
            if not isinstance(pysparksql_df, pyspark.sql.connect.dataframe.DataFrame):
                raise ValueError(f"Unexpected type;{type(pysparksql_df)}")

        pandas_df = pysparksql_df.toPandas()
        geometry_column = "geometry"
        if geometry_column in pysparksql_df.columns:
            pandas_df[geometry_column] = pandas_df[geometry_column].apply(wkt.loads)
            return gpd.GeoDataFrame(pandas_df, crs="EPSG:4326", geometry=geometry_column)
        else:
            return pandas_df

    @staticmethod
    def pysparksql_to_geopandas(
        pysparksql_df: pyspark.sql.DataFrame = None,
    ) -> gpd.GeoDataFrame:
        if "true" == os.environ.get("CI"):
            if not isinstance(pysparksql_df, pyspark.sql.DataFrame):
                raise ValueError(f"Unexpected type;{type(pysparksql_df)}")
        elif "true" != os.environ.get("LOCAL"):
            if not isinstance(pysparksql_df, pyspark.sql.DataFrame):
                raise ValueError(f"Unexpected type;{type(pysparksql_df)}")
        else:

            if not isinstance(pysparksql_df, pyspark.sql.connect.dataframe.DataFrame):
                raise ValueError(f"Unexpected type;{type(pysparksql_df)}")

        geometry_column = "geometry"
        pandas_df = pysparksql_df.toPandas()
        pandas_df[geometry_column] = pandas_df[geometry_column].apply(wkt.loads)

        return gpd.GeoDataFrame(pandas_df, crs="EPSG:4326", geometry=geometry_column)

    @staticmethod
    def geopandas_to_pysparksql(gpd_df: gpd.GeoDataFrame = None, spark: SparkSession = None) -> pyspark.sql.DataFrame:
        if not isinstance(gpd_df, gpd.GeoDataFrame):
            raise ValueError(f"Unexpected type;{type(gpd_df)}")

        if len(gpd_df) > 0:
            if "SHAPE" in gpd_df.columns:
                gpd_df = gpd_df.drop(columns=["SHAPE"])

            STANDARD_GEOMETRY_FIELD = "geometry"
            if STANDARD_GEOMETRY_FIELD in gpd_df.columns:
                gpd_df[STANDARD_GEOMETRY_FIELD] = gpd_df[STANDARD_GEOMETRY_FIELD].apply(lambda x: wkt.dumps(x))

            STANDARD_GEOMETRY_FIELD_X = "geometry_x"
            if STANDARD_GEOMETRY_FIELD_X in gpd_df.columns:
                gpd_df[STANDARD_GEOMETRY_FIELD_X] = gpd_df[STANDARD_GEOMETRY_FIELD_X].apply(lambda x: wkt.dumps(x))

            STANDARD_GEOMETRY_FIELD_Y = "geometry_y"
            if STANDARD_GEOMETRY_FIELD_Y in gpd_df.columns:
                gpd_df[STANDARD_GEOMETRY_FIELD_Y] = gpd_df[STANDARD_GEOMETRY_FIELD_Y].apply(lambda x: wkt.dumps(x))

            return spark.createDataFrame(pd.DataFrame(gpd_df))
        else:
            return PandasHelper.empty_spark_sql_dataframe(spark)

    @staticmethod
    def empty_spark_sql_dataframe(spark: SparkSession) -> pyspark.sql.DataFrame:
        schema = StructType([StructField("id", IntegerType(), True)])
        return spark.createDataFrame([], schema)

    @staticmethod
    def pandas_to_pysparksql(pd_df: pd.DataFrame = None, spark: SparkSession = None) -> pyspark.sql.DataFrame:
        if not isinstance(pd_df, pd.DataFrame):
            raise ValueError(f"Unexpected type;{type(pd_df)}")

        if len(pd_df) == 0:
            return PandasHelper.empty_spark_sql_dataframe(spark)
        else:
            return spark.createDataFrame(pd_df)

    @staticmethod
    def pysparksql_to_pandasDf(
        pysparksql_df: pyspark.sql.DataFrame = None,
    ) -> pd.DataFrame:
        if "true" == os.environ.get("CI"):
            if not isinstance(pysparksql_df, pyspark.sql.DataFrame):
                raise ValueError(f"Unexpected type;{type(pysparksql_df)}")
        elif "true" != os.environ.get("LOCAL"):
            if not isinstance(pysparksql_df, pyspark.sql.DataFrame):
                raise ValueError(f"Unexpected type;{type(pysparksql_df)}")
        else:
            if not isinstance(pysparksql_df, pyspark.sql.connect.dataframe.DataFrame):
                raise ValueError(f"Unexpected type;{type(pysparksql_df)}")

        pd_df = pysparksql_df.toPandas()
        return pd_df

    @staticmethod
    def to_pysparksql(pd_df, spark: SparkSession = None) -> pyspark.sql.DataFrame:
        if isinstance(pd_df, pd.DataFrame):
            return PandasHelper.pandas_to_pysparksql(pd_df, spark)
        elif isinstance(pd_df, gpd.GeoDataFrame):
            return PandasHelper.geopandas_to_pysparksql(pd_df, spark)
        else:
            raise ValueError(f"Unexpected type;{type(pd_df)}")

    @staticmethod
    def contains_geometry(pd_df, spark: SparkSession = None) -> bool:
        return "geometry" in pd_df.columns

    @staticmethod
    def to_pandas(my_dict):
        return pd.DataFrame(my_dict)
