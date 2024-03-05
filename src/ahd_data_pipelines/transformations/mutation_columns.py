from ahd_data_pipelines.transformations.transformation import Transformation
from pyspark.sql.functions import col, upper, lower


class MutateColumns(Transformation):
    def to_perform(params):
        return "mutations" in params.keys()

    def execute(dataFrame, params: dict = None, spark=None):
        mutations = params["mutations"]
        for mutation in mutations:
            print(f"Mutation - {mutation}")

            if mutation["mutation"] == "uppercase":
                dataFrame = dataFrame.withColumn(
                    mutation["column"], upper(col(mutation["column"]))
                )
            elif mutation["mutation"] == "lowercase":
                dataFrame = dataFrame.withColumn(
                    mutation["column"], lower(col(mutation["column"]))
                )
            else:
                print(f"Unknown mutation {mutation}")
        return dataFrame
