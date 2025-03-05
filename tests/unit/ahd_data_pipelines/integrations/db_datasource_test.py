import pytest
from pyspark.sql import SparkSession
from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType
from unittest.mock import Mock, patch
from ahd_data_pipelines.integrations.db_datasource import DatabricksDatasource
import logging

@pytest.fixture
def mock_spark():
    """Create a mock spark session with necessary attributes"""
    mock = Mock()
    mock.sql = Mock()
    mock.read = Mock()
    mock.read.table = Mock()
    mock.getActiveSession = Mock(return_value=mock)
    return mock

@pytest.fixture
def mock_dataframe():
    """Create a mock dataframe with a proper schema"""
    df = Mock()
    schema = StructType([StructField("existing_col", StringType(), True)])
    df.schema = schema
    df.count = Mock(return_value=1)
    df.write = Mock()
    df.write.mode = Mock(return_value=df.write)
    df.write.format = Mock(return_value=df.write)
    df.write.option = Mock(return_value=df.write)
    df.write.saveAsTable = Mock()
    df.__getitem__ = Mock()
    return df

def test_read_nonexistent_table(mock_spark):
    """Test that reading a nonexistent table raises RuntimeError"""
    table_name = "nonexistent_table"
    datasource = DatabricksDatasource({"table": table_name}, mock_spark)
    ae = AnalysisException("Table or view not found")
    mock_spark.read.table.side_effect = ae
    
    with pytest.raises(RuntimeError) as exc_info:
        datasource.read()
    print(f"Actual error message: {str(exc_info.value)}")
    assert "Failed to read table" in str(exc_info.value)

def test_read_invalid_query(mock_spark):
    """Test that an invalid SQL query raises RuntimeError"""
    query = "SELECT * FROM nonexistent_table"
    datasource = DatabricksDatasource({"query": query}, mock_spark)
    ae = AnalysisException("Table or view not found")
    mock_spark.sql.side_effect = ae
    
    with pytest.raises(RuntimeError) as exc_info:
        datasource.read()
    print(f"Actual error message: {str(exc_info.value)}")
    assert "Failed to execute query" in str(exc_info.value)

def test_truncate_nonexistent_table(mock_spark):
    """Test that truncating a nonexistent table raises RuntimeError with expected message"""
    with patch.dict('os.environ', {'LOCAL': 'true'}):
        table_name = "nonexistent_table"
        datasource = DatabricksDatasource({"table": table_name}, mock_spark)
        ae = AnalysisException("Table has 0 columns or does not exist, nothing to truncate")
        mock_spark.sql.side_effect = ae

        with pytest.raises(AnalysisException) as exc_info:
            datasource.truncate()
        
        assert "Table has 0 columns or does not exist, nothing to truncate" in str(exc_info.value)
