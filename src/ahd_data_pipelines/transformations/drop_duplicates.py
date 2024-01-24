from cdii_data_pipelines.transformations.transformation import Transformation


class DropDuplicates(Transformation):
    def to_perform(params):
        return 'drop_duplicates' in params.keys()

    def execute(dataFrame, params: dict = None, spark=None):
        cols_to_consider = params['drop_duplicates']
        if len(cols_to_consider) > 0:
            return dataFrame.dropDuplicates(cols_to_consider)
        else:
            return dataFrame.dropDuplicates()
