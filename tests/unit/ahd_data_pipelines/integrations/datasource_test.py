from ahd_data_pipelines.integrations.datasource import Datasource
from pyspark.sql import DataFrame
import pytest

class ConcreteDatasource(Datasource):
    def __init__(self, params: dict = None):
      super(ConcreteDatasource, self).__init__(params=params)

    def read(self) -> DataFrame:
      return None

def test_write_raises_notimplementederror():
    concreteDatasource = ConcreteDatasource()
    with pytest.raises(NotImplementedError) as theerror:
       concreteDatasource.write(dataFrame=None)

    assert str(theerror.value).startswith("'write' is not implemented") is True

def test_truncate_raises_notimplementederror():
    concreteDatasource = ConcreteDatasource()
    with pytest.raises(NotImplementedError) as theerror:
       concreteDatasource.truncate()
    
    assert str(theerror.value).startswith("'truncate' is not implemented") is True