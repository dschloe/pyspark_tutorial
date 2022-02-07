from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

def main():
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    # Create schema when reading u.logs
    schema = StructType([ \
                         StructField("userID", IntegerType(), True), \
                         StructField("movieID", IntegerType(), True), \
                         StructField("rating", IntegerType(), True), \
                         StructField("timestamp", LongType(), True)])

    # Load up movie logs as dataframe
    moviesDF = spark.read.option("sep", "\t").schema(schema).csv("data/ml-100k/u.logs")

    # Some SQL-style magic to sort all movies by popularity in one line!
    topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

    # Grab the top 10
    topMovieIDs.show(10)

    # Stop the session
    spark.stop()

if __name__ == "__main__":
    main()