from ahd_data_pipelines.transformations.drop_duplicates import DropDuplicates
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def test_to_perform_returns_true_appropriately():
    params = {
        "type": "noop",
        "drop_duplicates": True
    }    

    assert DropDuplicates.to_perform(params) == True

def test_to_perform_returns_false_appropriately_when_not_specified():
    params = {
        "type": "noop"
    }    

    assert DropDuplicates.to_perform(params) == False

def test_drops_dupes_when_needed():
    data = [(1,"a","b","c"),
            (1,"d","e", "f"),
            (3,"g","h", "i")]

    df = spark.createDataFrame(data,["id","a","b", "IrwinId"])

    params = {
        "type": "noop",
        "drop_duplicates": [
          "id"
        ]
    }  

    transformed_df = DropDuplicates.execute(df, params)

    assert transformed_df.count() is 2

def test_drops_dupes_when_using_all():
    data = [(1,"a","b","c"),
            (1,"a","b", "c"),
            (3,"g","h", "i")]

    df = spark.createDataFrame(data,["id","a","b", "IrwinId"])

    params = {
        "type": "noop",
        "drop_duplicates": []
    }  

    transformed_df = DropDuplicates.execute(df, params)

    assert transformed_df.count() is 2

def test_drops_dupes_when_using_three():
    data = [(1,"a","b","b"),
            (1,"a","b", "c"),
            (3,"g","h", "i")]

    df = spark.createDataFrame(data,["id","a","b", "IrwinId"])

    params = {
        "type": "noop",
        "drop_duplicates": [
          "id",
          "a",
          "b"
        ]
    }  

    transformed_df = DropDuplicates.execute(df, params)

    assert transformed_df.count() is 2

