from ahd_data_pipelines.transformations.drop_duplicates import DropDuplicates
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# tests if 'drop_duplicates' is in params, should return true
def test_to_perform_returns_true_appropriately():
    #setup
    params = {
      "drop_duplicates":[]
    }

    #execute
    test_value = DropDuplicates.to_perform(params)

    #execute
    assert test_value == True

# tests if 'drop_duplicates' is in params, should return false
def test_to_perform_returns_false_appropriately():
    #setup
    params = {
      "drop_columns":[]
    }

    #execute
    test_value = DropDuplicates.to_perform(params)

    #execute
    assert test_value == False

# test one row, no duplicates
def test_one_row_no_dupes():
    # setup
    params = {
      "drop_duplicates": []
    }

    data = [("John", 25)]

    # Define the schema for the DataFrame
    schema = ["name", "age"]

    # Create a DataFrame from the data and schema
    df = spark.createDataFrame(data, schema)

    # execute
    transformed_df = DropDuplicates.execute(df, params, spark)

    # #validate
    assert 2 == len(transformed_df.columns)
    assert 1 == transformed_df.count()

# test two rows, no duplicates
def test_two_rows_no_dupes():
    # setup
    params = {
      "drop_duplicates": []
    }

    data = [("John", 25), ("Alice", 30)]

    # Define the schema for the DataFrame
    schema = ["name", "age"]

    # Create a DataFrame from the data and schema
    df = spark.createDataFrame(data, schema)

    # execute
    transformed_df = DropDuplicates.execute(df, params, spark)

    # #validate
    assert 2 == len(transformed_df.columns)
    assert 2 == transformed_df.count()

# test three rows, a set of duplicates
def test_three_rows_with_set_of_dupes():
    # setup
    params = {
      "drop_duplicates": []
    }

    data = [("John", 25), ("Alice", 30), ("Alice", 30)]

    # Define the schema for the DataFrame
    schema = ["name", "age"]

    # Create a DataFrame from the data and schema
    df = spark.createDataFrame(data, schema)

    # execute
    transformed_df = DropDuplicates.execute(df, params, spark)

    # #validate
    assert 2 == len(transformed_df.columns)
    assert 2 == transformed_df.count()


# test four rows, two set of duplicates
def test_three_rows_with_set_of_dupes():
    # setup
    params = {
      "drop_duplicates": []
    }

    data = [("John", 25), ("John", 25), ("Alice", 30), ("Alice", 30)]

    # Define the schema for the DataFrame
    schema = ["name", "age"]

    # Create a DataFrame from the data and schema
    df = spark.createDataFrame(data, schema)

    # execute
    transformed_df = DropDuplicates.execute(df, params, spark)

    # #validate
    assert 2 == len(transformed_df.columns)
    assert 2 == transformed_df.count()
