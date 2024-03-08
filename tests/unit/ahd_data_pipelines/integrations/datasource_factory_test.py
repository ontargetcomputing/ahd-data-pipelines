
from ahd_data_pipelines.integrations.datasource_factory import DatasourceFactory
from ahd_data_pipelines.integrations.db_datasource import DatabricksDatasource
from ahd_data_pipelines.integrations.agol_datasource import AgolDatasource
from ahd_data_pipelines.integrations.datasource_type import DatasourceType

def test_default_datasource_is_databricks():
    assert type(DatasourceFactory.getDatasource()) is DatabricksDatasource

def test_databricks_returned_when_constructed():
    assert type(DatasourceFactory.getDatasource(type=DatasourceType.DATABRICKS)) is DatabricksDatasource

def test_agol_returned_when_constructed():
    params = {
      "url": "https://chhsagency.maps.arcgis.com/home/"
    }
    assert type(DatasourceFactory.getDatasource(type=DatasourceType.AGOL, params=params)) is AgolDatasource