import pyspark
from pyspark.sql import SparkSession


spark=SparkSession.builder.master("local[1]").appName('Pi-Estimation').getOrCreate()

import random
NUM_SAMPLES=1

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

count = spark.sparkContext.parallelize(range(0,NUM_SAMPLES)).filter(inside).count()
print("Pi is roughly {}".format(4.0 * count / NUM_SAMPLES))



