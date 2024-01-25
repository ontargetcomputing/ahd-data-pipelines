from ahd_data_pipelines.transformations.transformation import Transformation
from pyspark.sql.functions import regexp_replace


class StringReplace(Transformation):
    def to_perform(params):
        return (('string_replace' in params.keys()) and (len(params['string_replace']) > 0))

    def execute(dataFrame, params: dict = None, spark=None):
        string_replace = params['string_replace']
        print(f'Doing string replacements {string_replace}')
        for string_replacement in string_replace:
            values = string_replacement.rsplit(':')
            column = values[0]
            pattern = values[1]
            replacement = values[2]
            dataFrame = dataFrame.withColumn(column, regexp_replace(column, pattern, replacement))

        return dataFrame
