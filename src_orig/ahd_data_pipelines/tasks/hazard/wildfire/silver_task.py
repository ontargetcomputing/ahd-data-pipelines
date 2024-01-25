from cdii_data_pipelines.tasks.medallion_etl_task import MedallionETLTask
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, explode, regexp_replace
from array import array

IRWINID = "IRWINID"


class WildfireSilverTask(MedallionETLTask):
    def __init__(self, spark: SparkSession = None, init_conf: dict = None):
        super(WildfireSilverTask, self).__init__(spark=spark, init_conf=init_conf)

    def custom_transform(self, dataFrames: array, params: dict = None) -> array:
        print("Custom Transforming")
        new_dataFrames = []
        dataFrame = dataFrames[0]
        dataFrame = WildfireSilverTask._unnest(dataFrame)
        new_dataFrames.append(dataFrame)
        return new_dataFrames

    @staticmethod
    def _unnest(dataFrame: DataFrame = None) -> DataFrame:
        print(f'Unnest: started with {dataFrame.count()} records')
        if dataFrame.count() > 0 and IRWINID in dataFrame.columns:
            cooked_df = dataFrame.withColumn(IRWINID, explode(split(IRWINID, ',')))
            cooked_df = cooked_df.withColumn(IRWINID, regexp_replace(IRWINID, '\\{|\\}', ''))
            print(f'Unnest: ended with {cooked_df.count()} records')
            return cooked_df
        else:
            print(f'No unnesting to perform: count{dataFrame.count()}, {IRWINID} exits: {IRWINID in dataFrame.columns}')
            return dataFrame


def entrypoint():  # pragma: no cover
    task = WildfireSilverTask(init_conf=None)
    task.launch()


if __name__ == '__main__':
    entrypoint()
