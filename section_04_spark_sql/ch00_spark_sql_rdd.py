# Import logs types
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def main():
    # Create a SparkSession
    spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

    # Load a text file and convert each line to a Row.
    lines = spark.sparkContext.textFile("logs/people.txt")
    parts = lines.map(lambda l: l.split(","))

    # Each line is converted to a tuple.
    people = parts.map(lambda p: (p[0], p[1].strip()))

    # The schema is encoded in a string.
    schemaString = "name age"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaPeople = spark.createDataFrame(people, schema)

    # Creates a temporary view using the DataFrame
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    results = spark.sql("SELECT name FROM people")

    results.show()

    spark.stop()

if __name__ == "__main__":
    main()