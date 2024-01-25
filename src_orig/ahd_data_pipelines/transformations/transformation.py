from abc import abstractmethod


class Transformation(object):
    # def transform(self, dataFrame, params):
    #     if params.get(self.get_name(), False) is True:
    #         self.execute(dataFrame, params)

    @abstractmethod
    def execute(dataFrame, params, spark=None):
        pass

    @abstractmethod
    def to_perform(params):
        pass
