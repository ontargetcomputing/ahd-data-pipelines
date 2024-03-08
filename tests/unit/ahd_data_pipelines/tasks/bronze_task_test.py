from ahd_data_pipelines.tasks.medallion_etl_task import MedallionETLTask
from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
import logging
import pytz

spark = SparkSession.builder.getOrCreate()

default_params = {
    "bronze": True,
    "source_datasources": [
      {
      "type": "noop",
      }
    ],
    "destination_datasources": [
      {
      "type": "noop",
      }
    ]
}

class ConcreteBronzeTask(MedallionETLTask):
    def __init__(self):
      super(ConcreteBronzeTask, self).__init__(init_conf=default_params)

    def extract(self, params: dict=None) -> DataFrame:
        return None


def test_transform_adds_ade_submitted_date_to_today():
    logging.info("Ensure transform adds the ade_submitted_date")
    df = pd.DataFrame()
    df['firstname']=['Spongebog', 'Patrick']
    df['lastname']=['Squarepants', 'Star']
    df['age']=[12, 13]

    sdf = spark.createDataFrame(df)
    dfs = []
    dfs.append(sdf)
    test_bronze_task = ConcreteBronzeTask()

    dfs = test_bronze_task.transform(dataFrames=dfs, params=default_params)
    
    today = datetime.now(pytz.timezone("America/Los_Angeles")).date()
    assert True, 'ade_submitted_date' in df.columns

    assert True, (dfs[0]['ade_submitted_date'] == today).all()

