from ahd_data_pipelines.transformations.convert_zpoint import ConvertZPoint
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# note, we haven't done any tests to ensure each string_replace values is 3 colon delimited values
def test_to_perform_returns_true_appropriately():
    params = {
        "type": "noop",
        "convert_zpoint": "abc"
    }    

    assert ConvertZPoint.to_perform(params) == True

def test_to_perform_returns_false_appropriately_when_not_specified():
    params = {
        "type": "noop"
    }

    assert ConvertZPoint.to_perform(params) == False

