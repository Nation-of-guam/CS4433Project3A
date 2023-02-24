from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()

# Load the input CSV file as an RDD and select the 'id' column
people_rdd = spark.sparkContext.textFile("PEOPLE-SOME-INFECTED-large.csv") \
    .map(lambda line: line.split(",")) \
    .filter(lambda row: row[3] == "no") \
    .map(lambda row: row[0])

# Show the resulting RDD
people_rdd.foreach(print)
