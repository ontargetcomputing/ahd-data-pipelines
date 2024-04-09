from ahd_data_pipelines.transformations.epoch_to_timestamp import EpochToTimestamp
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

def test_to_perform_returns_true_appropriately():
    params = {
        "type": "noop",
        "epoch_to_timestamp": [
            "abc"
        ]
    }    

    assert EpochToTimestamp.to_perform(params) == True

def test_to_perform_returns_false_appropriately_when_empty_list():
    params = {
        "type": "noop",
        "epoch_to_timestamp": []
    }    

    assert EpochToTimestamp.to_perform(params) == False

def test_to_perform_returns_false_appropriately_when_not_specified():
    params = {
        "type": "noop"
    }

    assert EpochToTimestamp.to_perform(params) == False

def test_converts_appropriately():
    data = [(1,"a","b","1685063924")]

    df = spark.createDataFrame(data,["id","a","b", "myepoch"])

    params = {
        "type": "noop",
        "epoch_to_timestamp": [
          "myepoch"
        ]
    }  

    transformed_df = EpochToTimestamp.execute(df, params)

    assert type(transformed_df.select("myepoch").first()[0]) == datetime

def test_converts_appropriately_when_larger():
    data = [(1,"a","b",1685063924444)]

    df = spark.createDataFrame(data,["id","a","b", "myepoch"])

    params = {
        "type": "noop",
        "epoch_to_timestamp": [
          "myepoch"
        ]
    }  

    transformed_df = EpochToTimestamp.execute(df, params)

    assert type(transformed_df.select("myepoch").first()[0]) == datetime
