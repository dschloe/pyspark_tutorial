# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SampleTutorial").getOrCreate()
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
print("rdd Count:", rdd.count())