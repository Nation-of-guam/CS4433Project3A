from pyspark import SparkContext
from pyspark.sql import SparkSession

# Create SparkContext
sc = SparkContext("local", "RDD Example")

# Create SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

# Read CSV file as RDD
rdd = sc.textFile("PEOPLE-SOME-INFECTED-large.csv").map(lambda x: x.split(","))

# Filter out infected people and count
infected_count = rdd.filter(lambda x: x[3] == "yes").count()

# Print result
print("Number of infected people:", infected_count)