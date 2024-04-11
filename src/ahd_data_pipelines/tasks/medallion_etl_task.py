from ahd_data_pipelines.tasks.etl_task import ETLTask
from ahd_data_pipelines.transformations.drop_columns import DropColumns
from ahd_data_pipelines.transformations.rename_columns import RenameColumns
from ahd_data_pipelines.transformations.drop_duplicates import DropDuplicates
from ahd_data_pipelines.transformations.mutation_columns import MutateColumns
from ahd_data_pipelines.transformations.epoch_to_timestamp import EpochToTimestamp
from ahd_data_pipelines.transformations.string_replace import StringReplace
from ahd_data_pipelines.transformations.convert_zpoint import ConvertZPoint
from ahd_data_pipelines.transformations.fillna import FillNa
from ahd_data_pipelines.transformations.to_crs import ToCRS
from ahd_data_pipelines.pandas.geopandas_wrapper import GeoPandasWrapper
import pytz
from ahd_data_pipelines.pandas.pandas_helper import PandasHelper
from pyspark.sql import SparkSession
from array import array
from pyspark.sql.functions import lit
from datetime import datetime
import os
import pandas as pd
import geopandas as gpd
import json


class MedallionETLTask(ETLTask):
    """
    MedallionETLTask is an abstract class for the Medallion Arch.  This is the base of all Platform jobs.
    """

    def __init__(self, spark: SparkSession = None, init_conf: dict = None):
        super(MedallionETLTask, self).__init__(spark=spark, init_conf=init_conf)

    def transform(self, dataFrames: array, params: dict = None) -> array:
        """
        Transform method - used to transform data received from source in preparation of loading
        :return:
        """
        if self.conf.get("bronze", False) is False:
            print("Not Bronze - doing transformations")
            # dataFrames = self.__query(dataFrames=dataFrames, params=self.conf)
            dataFrames = self.__union(dataFrames=dataFrames, params=self.conf)
            dataFrames = self.__join(dataFrames=dataFrames, params=self.conf)
            dataFrames = self.__spatial_join(dataFrames=dataFrames, params=self.conf)
            dataFrames = self.custom_transform(dataFrames=dataFrames, params=self.conf)
            dataFrames = self.__outbound_transform(dataFrames=dataFrames, params=self.conf)
            new_dataFrames = dataFrames
        else:
            print('Is Bronze - only adding "ade_date_submitted" and renanming columns')
            transformations = [
                RenameColumns,
            ]
            print(params)
            new_dataFrames = []
            for index, dataFrame in enumerate(dataFrames):
                this_params = params["destination_datasources"][index]
                dataFrame = dataFrame.withColumn(
                    "ade_date_submitted",
                    lit(datetime.now(pytz.timezone("America/Los_Angeles")).date()),
                )
                print(f"Transforming {this_params}")
                dataFrame = self.__do_outbound_transform(
                    transformations=transformations,
                    dataFrame=dataFrame,
                    params=this_params,
                )
                new_dataFrames.append(dataFrame)

        return new_dataFrames

    def custom_transform(self, dataFrames: array, params: dict = None) -> array:
        print("No custom transformations, reducing dataFrames to first dataFrame only")
        return dataFrames[0:1]

    def __do_outbound_transform(self, transformations, dataFrame, params):
        for cls in transformations:
            if cls.to_perform(params) is True:
                print(f"Executing {cls.__name__}")
                dataFrame = cls.execute(dataFrame, params, self.spark)
            else:
                print(f"Skipping {cls.__name__}")

        return dataFrame

    def __outbound_transform(self, dataFrames, params: dict = None) -> array:
        new_dataFrames = []
        for index, dataFrame in enumerate(dataFrames):
            if dataFrame.count() > 0:
                this_params = params["destination_datasources"][index]
                print(f"Transforming {this_params}")

                transformations = [
                    DropColumns,
                    DropDuplicates,
                    MutateColumns,
                    EpochToTimestamp,
                    StringReplace,
                    ConvertZPoint,
                    ToCRS,
                    FillNa,
                    RenameColumns,
                ]
                dataFrame = self.__do_outbound_transform(
                    transformations=transformations,
                    dataFrame=dataFrame,
                    params=this_params,
                )
            else:
                print("Dataframe is empty, no outbound transformation needed")

            new_dataFrames.append(dataFrame)
        return new_dataFrames

    def __spatial_join(self, dataFrames, params: dict = None) -> array:
        if "spatial_joins" in params:
            joins = params["spatial_joins"]
            print(f"spatial_joins is {joins}")
            for join in joins:
                print(f"Performing sjoins with - {join}")

                predicate = join["predicate"]
                how = join["how"]
                left_index = join["left_index"]
                right_index = join["right_index"]
                drop_columns = join["drop_columns"]
                left_pdf = dataFrames[left_index]
                right_pdf = dataFrames[right_index]
                if left_pdf.count() == 0 or right_pdf.count() == 0:
                    print("We have an empty dataset, nothing to spatial join")
                else:
                    print("Columns in Left:")
                    for col in left_pdf.columns:
                        print(col)
                    print("Columns in Right:")
                    for col in right_pdf.columns:
                        print(col)
                    left_df = PandasHelper.pysparksql_to_geopandas(left_pdf)
                    right_df = PandasHelper.pysparksql_to_geopandas(right_pdf)

                    joined = GeoPandasWrapper.sjoin(left_df, right_df, how=how, predicate=predicate)
                    joined.drop(columns=drop_columns, inplace=True)
                    dataFrames[left_index] = PandasHelper.geopandas_to_pysparksql(joined, spark=self.spark)

        return dataFrames

    def __union(self, dataFrames, params: dict = None) -> array:
        if "unions" in params:
            unions = params["unions"]
            print(f"Unions is {unions}")
            for union in unions:
                print(f"Performing union with - {union}")
                left_index = union["left_index"]
                right_index = union["right_index"]
                left_pdf = dataFrames[left_index]
                right_pdf = dataFrames[right_index]
                unioned_df = left_pdf.union(right_pdf)
                dataFrames[left_index] = unioned_df
        return dataFrames

    def __join(self, dataFrames, params: dict = None) -> array:
        if "joins" in params:
            joins = params["joins"]
            print(f"Joins is {joins}")
            for join in joins:
                print(f"Performing join with - {join}")

                left_index = join["left_index"]
                right_index = join["right_index"]

                left_pdf = dataFrames[left_index]
                right_pdf = dataFrames[right_index]
                cols_to_mergs = join.get("merge_columns", None)
                if cols_to_mergs is not None:
                    right_pdf = right_pdf[cols_to_mergs]
                left_count = left_pdf.count()
                right_count = right_pdf.count()
                if left_count == 0 or right_count == 0:
                    print("We have an empty dataset, nothing to join")
                else:
                    print(f"Merging {left_count} records from left with {right_count} records from right")
                    left_df = PandasHelper.pysparksql_to_pandas_or_geopandas(left_pdf)
                    right_df = PandasHelper.pysparksql_to_pandas_or_geopandas(right_pdf)
                    how = join.get("how", "left")
                    merged = pd.merge(
                        left_df,
                        right_df,
                        how=how,
                        on=join["join_on"],
                    )

                    print(f"After merge, columns are {merged.columns}")

                    transformations = [
                        DropColumns,
                        RenameColumns,
                    ]
                    merged = self.__do_outbound_transform(
                        transformations=transformations, dataFrame=merged, params=join
                    )
                    print(merged.head())
                    if isinstance(merged, pd.DataFrame) and PandasHelper.contains_geometry(merged):
                        merged = gpd.GeoDataFrame(merged, geometry="geometry")
                        dataFrames[left_index] = PandasHelper.geopandas_to_pysparksql(merged, spark=self.spark)
                    else:
                        dataFrames[left_index] = PandasHelper.pandas_to_pysparksql(merged, spark=self.spark)

        return dataFrames


def entrypoint():
    # pragma: no cover
    print("Running MedallionETLTask")

    if "true" != os.environ.get("LOCAL"):
        init_conf = None
    else:
        yaml_file_path = "./conf/parameters/hazard/wildfire/bronze_perims_task.yml"
        init_conf = ETLTask.load_yaml(yaml_file_path=yaml_file_path)
        print("******************************")
        print("LOADED CONFIG FROM YAML FILE LOCALLY")
        print(json.dumps(init_conf, indent=2))
        print("******************************")

    task = MedallionETLTask(init_conf=init_conf)

    task.launch()


if __name__ == "__main__":
    entrypoint()
