from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.master("local[1]").appName("MLSampleTutorial").getOrCreate()

# Import the data to a DataFrame
departures_df = spark.read.csv('data/AA_DFW_2015_Departures_Short.csv.gz', header=True)

# Remove any duration of 0
departures_df = departures_df.filter(departures_df[3] > 0)

# Add an ID column
departures_df = departures_df.withColumn('id', F.monotonically_increasing_id())

# departures_df.show()

# Write the file out to JSON format
departures_df.write.csv('data/output.csv', mode='overwrite')
