from cdii_data_pipelines.tasks.medallion_etl_task import MedallionETLTask
from cdii_data_pipelines.tasks.etl_task import ETLTask
from pyspark.sql import SparkSession
from array import array
import os
import json
from pyspark.sql.functions import lit


class ShelterSitesHistoricalGoldTask(MedallionETLTask):

    def __init__(self, spark: SparkSession = None, init_conf: dict = None):
        super(ShelterSitesHistoricalGoldTask, self).__init__(spark=spark, init_conf=init_conf)

    def custom_transform(self, dataFrames: array, params: dict = None) -> array:
        print("Doing custom transformation of ShelterSitesHistoricalGoldTask")

        final_dataFrames = []

        cs_delta = dataFrames[1].filter(dataFrames[1]['site_type'] == 'Congregate').first()['difference']
        ncs_delta = dataFrames[1].filter(dataFrames[1]['site_type'] == 'Non Congregate').first()['difference']
        dataFrames[0] = dataFrames[0].withColumn('cs_delta', lit(cs_delta))
        dataFrames[0] = dataFrames[0].withColumn('ncs_delta', lit(ncs_delta))

        final_dataFrames.append(dataFrames[0])
        return final_dataFrames


def entrypoint():  # pragma: no cover

    if "true" != os.environ.get('LOCAL'):
        init_conf = None
    else:
        yaml_file_path = './conf/tasks/shelter/report/gold_shelter_sites_historical.yml'
        init_conf = ETLTask.load_yaml(yaml_file_path=yaml_file_path)
        print("******************************")
        print("LOADED CONFIG FROM YAML FILE LOCALLY")
        print(json.dumps(init_conf, indent=2))
        print("******************************")

    task = ShelterSitesHistoricalGoldTask(init_conf=init_conf)
    task.launch()


if __name__ == '__main__':
    entrypoint()
