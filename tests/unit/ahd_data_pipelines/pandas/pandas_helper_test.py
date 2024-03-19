from pyspark.sql import SparkSession
from ahd_data_pipelines.pandas.pandas_helper import PandasHelper
import pandas as pd
import geopandas as gpd
import pytest
from shapely import wkt
from shapely import Point
import pyspark

spark = SparkSession.builder.getOrCreate()

def test_pysparksql_to_geopandas_requires_correct_type():
    with pytest.raises(ValueError):
        PandasHelper.pysparksql_to_geopandas(pd.DataFrame())

def test_pysparksql_to_geopandas_returns_correct_type():
    d = {'col1': ['name1', 'name2'], 'geometry': [Point(1, 2), Point(2, 1)]}
    gdf = gpd.GeoDataFrame(d, crs="EPSG:4326")

    if 'SHAPE' in gdf.columns:
        gdf = gdf.drop(columns=['SHAPE'])

    GEOMETRY_FIELD = 'geometry'
    gdf[GEOMETRY_FIELD] = gdf[GEOMETRY_FIELD].apply(lambda x: wkt.dumps(x))
    
    df = spark.createDataFrame(gdf)
    new_df = PandasHelper.pysparksql_to_geopandas(df)

    assert type(new_df) is gpd.GeoDataFrame

def test_geopandas_to_pysparksql_requires_correct_type():
    with pytest.raises(ValueError):
        PandasHelper.geopandas_to_pysparksql(pd.DataFrame())

def test_emtpy_spark_sql_dataframe_is_correct_type():
    df = PandasHelper.empty_spark_sql_dataframe(spark=spark)

    assert type(df) is pyspark.sql.DataFrame

def test_emtpy_spark_sql_dataframe_returns_empty():
    df = PandasHelper.empty_spark_sql_dataframe(spark=spark)

    assert df.count() == 0