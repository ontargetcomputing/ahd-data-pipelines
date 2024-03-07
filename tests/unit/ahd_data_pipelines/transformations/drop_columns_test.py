from ahd_data_pipelines.transformations.drop_columns import DropColumns
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# comment on this test so people understand what you are doing.
def test_to_perform_returns_true_appropriately():
    #setup
    params = {
      "drop_columns": [
        "days_within"
      ]
    }

    #execute
    test_value = DropColumns.to_perform(params)

    #execute
    assert test_value == True


def test_to_perform_returns_false_appropriately():
    #setup
    params = {
      "xyz_columns": [
        "days_within"
      ]
    }

    #execute
    test_value = DropColumns.to_perform(params)
    
    # validate
    assert test_value == False

def test_execute_works_with_single():
    # setup
    params = {
      "drop_columns": [
        "name"
      ]
    }

    data = [("John", 25), ("Alice", 30), ("Bob", 35)]

    # Define the schema for the DataFrame
    schema = ["name", "age"]

    # Create a DataFrame from the data and schema
    df = spark.createDataFrame(data, schema)


    # execute
    transformed_df = DropColumns.execute(df, params, spark)

    # #validate
    assert 1 == len(transformed_df.columns)
    assert "name" not in transformed_df.columns
    assert "age" in transformed_df.columns


def test_execute_with_multiple():
    # setup
    params = {
      "drop_columns": [
        "name","age"
      ]
    }

    data = [("John", 25, "M"), ("Alice", 30,"F"), ("Bob", 35,"M")]

    # Define the schema for the DataFrame
    schema = ["name", "age", "gender"]

    # Create a DataFrame from the data and schema
    df = spark.createDataFrame(data, schema)

    # execute
    transformed_df = DropColumns.execute(df, params, spark)

    # #validate
    assert 1 == len(transformed_df.columns)
    assert "name" not in transformed_df.columns
    assert "age" not in transformed_df.columns
    assert "gender" in transformed_df.columns

def test_execute_works_with_nothing():
   # setup
    params = {
      "drop_columns": []
    }

    data = [("John", 25, "M"), ("Alice", 30,"F"), ("Bob", 35,"M")]

    # Define the schema for the DataFrame
    schema = ["name", "age", "gender"]

    # Create a DataFrame from the data and schema
    df = spark.createDataFrame(data, schema)

    # execute
    transformed_df = DropColumns.execute(df, params, spark)

    # #validate
    assert 3 == len(transformed_df.columns)
    assert "name" in transformed_df.columns
    assert "age" in transformed_df.columns
    assert "gender" in transformed_df.columns
