# SparkContext:
# Spark framework to create RDD
# Can't Create SparkContext without SparkConf
# Configure
from pyspark import SparkConf, SparkContext
import collections


def main():
# MasterNode = local
# It's using Elastic MapReduce

    conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
    sc = SparkContext(conf = conf)

    # lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
    lines = sc.textFile("ml-100k/u.data")
    ratings = lines.map(lambda x: x.split()[2])
    print("ratings: ", ratings)
    result = ratings.countByValue()
    print("result: ", result)

    sortedResults = collections.OrderedDict(sorted(result.items()))
    for key, value in sortedResults.items():
        print("%s %i" % (key, value))

if __name__ == "__main__":
    main()