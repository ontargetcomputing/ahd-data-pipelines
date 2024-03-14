# from ahd_data_pipelines.tasks.hazard.wildfire.silver_task import WildfireSilverTask
# from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()


# def test_unnest_removes_braces():
#     data = [(1,"a","b","{abc-def}"),
#             (2,"d","e", "{ghi-jkl}"),
#             (3,"g","h", "{mno-pqr")]

#     df = spark.createDataFrame(data,["id","a","b", "IRWINID"])
    
#     transformed_df = WildfireSilverTask._unnest(df)
#     assert transformed_df.collect()[0]['IRWINID'] == 'abc-def'
#     assert transformed_df.collect()[1]['IRWINID'] == 'ghi-jkl'
#     assert transformed_df.collect()[2]['IRWINID'] == 'mno-pqr'

# def test_unnest_expands():
#     data = [(1,"a","b","{abc-def},{stu-vwx"),
#             (2,"d","e", "{ghi-jkl}"),
#             (3,"g","h", "{mno-pqr")]

#     df = spark.createDataFrame(data,["id","a","b", "IRWINID"])
    
#     transformed_df = WildfireSilverTask._unnest(df)
#     assert transformed_df.count() == 4
#     assert transformed_df.collect()[0]['IRWINID'] == 'abc-def'
#     assert transformed_df.collect()[1]['IRWINID'] == 'stu-vwx'
