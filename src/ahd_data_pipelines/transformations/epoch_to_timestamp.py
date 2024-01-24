from cdii_data_pipelines.transformations.transformation import Transformation
from pyspark.sql.functions import from_unixtime, to_timestamp
from pyspark.sql.functions import col, when, length


class EpochToTimestamp(Transformation):
    def to_perform(params):
        return (('epoch_to_timestamp' in params.keys()) and (len(params['epoch_to_timestamp']) > 0))

    def execute(dataFrame, params: dict = None, spark=None):
        epoch_to_timestamp = params['epoch_to_timestamp']
        print(f'Converting epoch to timestamps {epoch_to_timestamp}')
        for epoch_to_convert in epoch_to_timestamp:
            print(f'Converting column {epoch_to_convert}')
            dataFrame = dataFrame.withColumn(
                epoch_to_convert,
                when(
                    length(
                        col(epoch_to_convert)) == 13,
                    (col(epoch_to_convert) /
                     1000).cast("integer")).otherwise(
                    col(epoch_to_convert).cast("integer")))
            dataFrame = dataFrame.withColumn(epoch_to_convert, to_timestamp(from_unixtime(epoch_to_convert)))

        return dataFrame
