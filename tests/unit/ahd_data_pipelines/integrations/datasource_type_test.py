import unittest
from ahd_data_pipelines.integrations.datasource_type import DatasourceType

class TestDatasourceType(unittest.TestCase):
    def test_enum_values(self):
        self.assertEqual(DatasourceType.NOOP.value, "noop")
        self.assertEqual(DatasourceType.AGOL.value, "agol")
        self.assertEqual(DatasourceType.DATABRICKS.value, "databricks")
        self.assertEqual(DatasourceType.S3.value, "s3")
        self.assertEqual(DatasourceType.HTTP.value, "http")
        self.assertEqual(DatasourceType.FILE.value, "file")
        self.assertEqual(DatasourceType.JDBC.value, "jdbc")
        self.assertEqual(DatasourceType.PANDAS.value, "pandas")

    def test_enum_names(self):
        self.assertEqual(DatasourceType.NOOP.name, "NOOP")
        self.assertEqual(DatasourceType.AGOL.name, "AGOL")
        self.assertEqual(DatasourceType.DATABRICKS.name, "DATABRICKS")
        self.assertEqual(DatasourceType.S3.name, "S3")
        self.assertEqual(DatasourceType.HTTP.name, "HTTP")
        self.assertEqual(DatasourceType.FILE.name, "FILE")
        self.assertEqual(DatasourceType.JDBC.name, "JDBC")
        self.assertEqual(DatasourceType.PANDAS.name, "PANDAS")

if __name__ == "__main__":
    unittest.main()
