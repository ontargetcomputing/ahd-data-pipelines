from ahd_data_pipelines.transformations.string_replace import StringReplace
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# note, we haven't done any tests to ensure each string_replace values is 3 colon delimited values
def test_to_perform_returns_true_appropriately():
    params = {
        "type": "noop",
        "string_replace": [
            "abc:bbb:ccc"
        ]
    }    

    assert StringReplace.to_perform(params) == True

def test_to_perform_returns_false_appropriately_when_empty_list():
    params = {
        "type": "noop",
        "string_replace": []
    }    

    assert StringReplace.to_perform(params) == False

def test_to_perform_returns_false_appropriately_when_not_specified():
    params = {
        "type": "noop"
    }

    assert StringReplace.to_perform(params) == False

def test_string_replaces_appropriately():
    data = [(1,"aba",",b,","c")]

    df = spark.createDataFrame(data,["id","one","two","three"])

    params = {
        "type": "noop",
        "string_replace": [
          "one:a:f",
          "two:^,|,$:"
        ]
    }  

    transformed_df = StringReplace.execute(df, params)

    expected_datetime = datetime.strptime("2023-05-25 18:18:44", "%Y-%m-%d %H:%M:%S")

    assert transformed_df.select("one").first()[0] == 'fbf'
    assert transformed_df.select("two").first()[0] == 'b'
