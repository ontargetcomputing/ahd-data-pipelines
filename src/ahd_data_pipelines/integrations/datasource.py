from pyspark.sql import DataFrame
from abc import ABC, abstractmethod


class Datasource(ABC):
    """ """

    def __init__(self, params: dict = None):
        pass

    @abstractmethod
    def read(self) -> DataFrame:
        pass

    def write(self, dataFrame: DataFrame):
        raise NotImplementedError(
            """'write' is not implemented, please provide an implementation if your class
                is a writable datasource, otherwise ensure you are not attempting to write
                to this datasource""".replace(
                "\n", ""
            )
        )

    def truncate(self):
        raise NotImplementedError(
            """'truncate' is not implemented, please provide an implementation if your class
                is a writable datasource, otherwise ensure you are not attempting to write to this
                datasource""".replace(
                "\n", ""
            )
        )
