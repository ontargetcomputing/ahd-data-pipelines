import pytest
import io
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from ahd_data_pipelines.transformations.mutation_columns import MutateColumns
from pyspark.sql.functions import col, upper, lower


@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def input_data(spark_session):
    schema = StructType([
        StructField("name", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True),
    ])

    data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
    return spark_session.createDataFrame(data, schema)


def test_mutate_columns_uppercase(spark_session, input_data):
    input_data.createOrReplaceTempView("input_data")

    params = {
        "mutations": [
            {"mutation": "uppercase", "column": "name"}
        ]
    }

    transformed_df = MutateColumns.execute(input_data, params, spark_session)

    # Check if uppercase mutation is applied correctly
    expected_df = input_data.withColumn("name", upper(col("name")))
    assert transformed_df.collect() == expected_df.collect()


def test_mutate_columns_lowercase(spark_session, input_data):
    input_data.createOrReplaceTempView("input_data")

    params = {
        "mutations": [
            {"mutation": "lowercase", "column": "name"}
        ]
    }

    transformed_df = MutateColumns.execute(input_data, params, spark_session)

    # Check if lowercase mutation is applied correctly
    expected_df = input_data.withColumn("name", lower(col("name")))
    assert transformed_df.collect() == expected_df.collect()

def test_mutate_columns_unknown_mutation(spark_session, input_data):
    input_data.createOrReplaceTempView("input_data")

    params = {
        "mutations": [
            {"mutation": "unknown", "column": "name"}
        ]
    }

    # Redirecting stdout to capture printed output
    captured_output = io.StringIO()
    sys.stdout = captured_output

    MutateColumns.execute(input_data, params, spark_session)

    # Reset stdout
    sys.stdout = sys.__stdout__

    # Get printed output
    printed_text = captured_output.getvalue().strip()

    # Check if the expected message is printed
    expected_message = "Unknown mutation {'mutation': 'unknown', 'column': 'name'}"
    assert expected_message in printed_text
