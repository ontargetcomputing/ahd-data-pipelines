from ahd_data_pipelines.transformations.rename_columns import RenameColumns
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def test_to_perform_returns_true_appropriately():
    params = {
        "type": "noop",
        "rename_columns": [
            "a:b"
        ]
    }    

    assert RenameColumns.to_perform(params) == True

def test_to_perform_returns_false_appropriately_when_empty_list():
    params = {
        "type": "noop",
        "rename_columns": []
    }    

    assert RenameColumns.to_perform(params) == False

def test_to_perform_returns_false_appropriately_when_not_specified():
    params = {
        "type": "noop"
    }

    assert RenameColumns.to_perform(params) == False

def test_renames_when_needed():
    data = [(1,"a","b","c"),
            (2,"d","e", "f"),
            (3,"g","h", "i")]

    df = spark.createDataFrame(data,["id","a","b", "IrwinId"])

    params = {
        "type": "noop",
        "rename_columns": [
          "IrwinId:IRWINID"
        ]
    }  

    transformed_df = RenameColumns.execute(df, params)

    assert True, 'IrwinId' not in transformed_df.columns
    assert True, 'IRWINID' in transformed_df.columns

def test_create_upper_irwinid_no_converts_when_not_needed():
    data = [(1,"a","b","c"),
            (2,"d","e", "f"),
            (3,"g","h", "i")]

    df = spark.createDataFrame(data,["id","a","b", "c"])
    
    params = {
        "type": "noop",
        "rename_columns": []
    }

    transformed_df = RenameColumns.execute(df, params)
    assert True, 'IRWINID' not in transformed_df.columns
