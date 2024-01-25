from cdii_data_pipelines.tasks.medallion_etl_task import MedallionETLTask
from cdii_data_pipelines.tasks.etl_task import ETLTask
from pyspark.sql import SparkSession
from array import array
import os
import json
import pandas as pd
from cdii_data_pipelines.pandas.pandas_helper import PandasHelper

counties = {
    'county': [
        'Alameda', 'Amador', 'Calaveras', 'Contra Costa', 'Mendocino', 'Merced', 'Sacramento',
        'San Joaquin', 'San Luis Obispo', 'San Mateo', 'Santa Barbara', 'Ventura', 'Kern',
        'Mariposa', 'San Benito', 'San Bernardino', 'Tulare', 'Tuolumne', 'Monterey', 'Santa Cruz',
        'Alpine', 'Butte', 'Colusa', 'Del Norte', 'El Dorado', 'Fresno', 'Glenn', 'Humboldt',
        'Imperial', 'Inyo', 'Kings', 'Lake', 'Lassen', 'Los Angeles', 'Madera', 'Marin',
        'Modoc', 'Mono', 'Napa', 'Nevada', 'Orange', 'Placer', 'Plumas', 'Riverside',
        'San Diego', 'San Francisco', 'Santa Clara', 'Shasta', 'Sierra', 'Siskiyou', 'Solano',
        'Sonoma', 'Stanislaus', 'Sutter', 'Tehama', 'Trinity', 'Yolo', 'Yuba',
        'Hoopa Valley Indian Reservation'
    ]
}


class FemaIHPValidRegistrationsGreenTask(MedallionETLTask):

    def __init__(self, spark: SparkSession = None, init_conf: dict = None):
        super(FemaIHPValidRegistrationsGreenTask, self).__init__(spark=spark, init_conf=init_conf)

    def custom_transform(self, dataFrames: array, params: dict = None) -> array:
        print("Doing custom transformation of FemaIHPValidRegistrationsGreenTask")
        print(dataFrames[0].show())

        counties_df = PandasHelper.to_pandas(counties)
        aggregation_df = PandasHelper.pysparksql_to_pandasDf(dataFrames[0])
        merged = pd.merge(counties_df,
                          aggregation_df,
                          how="left",
                          on='county',
                          )

        print(merged.head())
        dataFrames = []
        dataFrames.append(PandasHelper.pandas_to_pysparksql(merged, self.spark))
        return dataFrames


def entrypoint():  # pragma: no cover

    if "true" != os.environ.get('LOCAL'):
        init_conf = None
    else:
        yaml_file_path = './conf/tasks/hazard/fema/green_ihp_valid_registrations_aggregation.ci.yml'
        init_conf = ETLTask.load_yaml(yaml_file_path=yaml_file_path)
        print("******************************")
        print("LOADED CONFIG FROM YAML FILE LOCALLY")
        print(json.dumps(init_conf, indent=2))
        print("******************************")

    task = FemaIHPValidRegistrationsGreenTask(init_conf=init_conf)
    task.launch()


if __name__ == '__main__':
    entrypoint()
