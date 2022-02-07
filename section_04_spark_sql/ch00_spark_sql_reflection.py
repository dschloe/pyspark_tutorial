# https://kks32-courses.gitbook.io/data-analytics/spark/spark-sql

from pyspark.sql import SparkSession
from pyspark.sql import Row

def main():
    # Create a SparkSession
    spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
    # Load a text file and convert each line to a Row.
    lines = spark.sparkContext.textFile("logs/people.txt")
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

    # Infer the schema, and register the DataFrame as a table.
    schemaPeople = spark.createDataFrame(people)
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    # The results of SQL queries are Dataframe objects.
    # rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
    teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
    for name in teenNames:
        print(name)

    spark.stop()

if __name__ == "__main__":
    main()