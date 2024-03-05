from ahd_data_pipelines.transformations.transformation import Transformation


class FillNa(Transformation):
    def to_perform(params):
        return "fillna" in params.keys()

    def execute(dataFrame, params: dict = None, spark=None):
        value = params.get("fillna", 0)
        dataFrame = dataFrame.fillna(value)
        return dataFrame
