from ahd_data_pipelines.tasks.medallion_etl_task import MedallionETLTask
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from ahd_data_pipelines.pandas.pandas_helper import PandasHelper
# from pyspark.sql.functions import isnan, isnull
from array import array


class DashboardEarthquakeEpicenterGreenTask(MedallionETLTask):

    def __init__(self, spark: SparkSession = None, init_conf: dict = None):
        super(DashboardEarthquakeEpicenterGreenTask, self).__init__(spark=spark, init_conf=init_conf)

    def custom_transform(self, dataFrames: array, params: dict = None) -> array:
        print(f"Doing custom transformation of earthquake data with {dataFrames[0].count()} records")
        dataFrame = dataFrames[0]
        # dataFrame = dataFrame.filter((dataFrame.mmi >= 6.0) | (dataFrame.mag > 5.6))

        gpd_df = PandasHelper.pysparksql_to_geopandas(dataFrame)
        # TODO - do not have time or desire right now to figure out how to do this
        # with a psypark.sql.dataframe.  Do that eventually
        rightnow = datetime.now()
        within_1_days = gpd_df['time'] + timedelta(days=1) >= rightnow
        within_3_days = gpd_df['time'] + timedelta(days=3) >= rightnow
        within_7_days = gpd_df['time'] + timedelta(days=7) >= rightnow
        within_10_days = gpd_df['time'] + timedelta(days=10) >= rightnow
        within_14_days = gpd_df['time'] + timedelta(days=14) >= rightnow
        within_30_days = gpd_df['time'] + timedelta(days=30) >= rightnow

        gpd_df = gpd_df[((gpd_df['mag'] >= 3.5) & (within_1_days)) |
                        (((gpd_df['mag'] >= 5.6) | (gpd_df['mmi'] >= 4.0)) & (within_3_days)) |
                        (((gpd_df['mag'] >= 7.1) | (gpd_df['mmi'] >= 6.0)) & (within_7_days)) |
                        ((gpd_df['mmi'] >= 6) & within_7_days) |
                        ((gpd_df['mmi'] >= 8.0) & (within_10_days)) |
                        ((gpd_df['mmi'] >= 9.0) & (within_14_days)) |
                        ((gpd_df['mmi'] >= 10.0) & (within_30_days))]

        print(f"Finished custom transformation of DashboardEarthquakeEpicenterGreenTask with {len(gpd_df)} records")
        dataFrames[0] = PandasHelper.geopandas_to_pysparksql(gpd_df, spark=self.spark)
        return dataFrames


def entrypoint():  # pragma: no cover
    task = DashboardEarthquakeEpicenterGreenTask(init_conf=None)
    task.launch()


if __name__ == '__main__':
    entrypoint()
