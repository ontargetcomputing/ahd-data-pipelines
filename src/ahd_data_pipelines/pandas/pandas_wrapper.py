import pandas as pd


class PandasWrapper:
    @staticmethod
    def notnull(dataFrame) -> pd.DataFrame:
        return pd.notnull(dataFrame)
