from ahd_data_pipelines.tasks.medallion_etl_task import MedallionETLTask
from ahd_data_pipelines.tasks.etl_task import ETLTask
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from array import array
import os
import json
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from shapely import wkt
from shapely.geometry import Polygon
import geopy
import geopy.distance

IRWINID = "IRWINID"


class WildfireGoldTask(MedallionETLTask):
    def __init__(self, spark: SparkSession = None, init_conf: dict = None):
        super(WildfireGoldTask, self).__init__(spark=spark, init_conf=init_conf)

    def custom_transform(self, dataFrames: array, params: dict = None) -> array:
        print("Custom Transforming")
        new_dataFrames = []
        dataFrame = dataFrames[0]
        dataFrame = WildfireGoldTask._create_polygons(dataFrame=dataFrame, spark=self.spark)

        new_dataFrames.append(dataFrame)
        return new_dataFrames

    @staticmethod
    def _create_poly_agol(geometry_x):
        print(geometry_x)
        point = wkt.loads(geometry_x)

        origin = geopy.Point(point.y, point.x)
        north_point = geopy.distance.distance(kilometers=0.1).destination(origin, 0)
        southeast_point = geopy.distance.distance(kilometers=0.1).destination(origin, 135)
        southwest_point = geopy.distance.distance(kilometers=0.1).destination(origin, 225)
        poly = Polygon(
            [
                [north_point.longitude, north_point.latitude],
                [southeast_point.longitude, southeast_point.latitude],
                [southwest_point.longitude, southwest_point.latitude],
            ]
        )

        poly_str = wkt.dumps(poly)
        return poly_str

    @staticmethod
    def _create_polygons(dataFrame: DataFrame = None, spark: SparkSession = None) -> DataFrame:
        print("Creating Polygons")
        my_udf = udf(WildfireGoldTask._create_poly_agol, StringType())
        full = dataFrame.filter(col("geometry").isNotNull() & (col("geometry") != ""))
        empty = dataFrame.filter(col("geometry").isNull() | (col("geometry") == ""))

        empty_fixed = empty.withColumn("geometry", my_udf(empty["geometry_x"]))

        return full.union(empty_fixed)
        # return dataFrame


def entrypoint():  # pragma: no cover

    if "true" != os.environ.get("LOCAL"):
        init_conf = None
    else:
        yaml_file_path = "./conf/tasks/hazard/wildfire/gold_perims_task.yml"
        init_conf = ETLTask.load_yaml(yaml_file_path=yaml_file_path)
        print("******************************")
        print("LOADED CONFIG FROM YAML FILE LOCALLY")
        print(json.dumps(init_conf, indent=2))
        print("******************************")

    task = WildfireGoldTask(init_conf=init_conf)
    task.launch()


if __name__ == "__main__":
    entrypoint()
