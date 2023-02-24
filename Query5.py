from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


# Create a SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()

schema2 = StructType([
    StructField("id", IntegerType()),
    StructField("x", IntegerType()),
    StructField("y", IntegerType()),
    StructField("INFECTED", StringType())
])

# Load the PEOPLE-large CSV file as an RDD and extract the (id, x, y) coordinates
people_rdd = spark.read.format("csv").option("header", True).schema(schema2).load("PEOPLE-SOME-INFECTED-large.csv") \
    .select("id", "x", "y", "INFECTED").rdd.map(tuple)

# Filter to only include infected people
infected_rdd = people_rdd.filter(lambda row: row[3] == "yes")

# Compute the number of people who are in close proximity to each infected person
close_contacts_rdd = infected_rdd.flatMap(lambda infect_i: \
        people_rdd.filter(lambda p_j: p_j[3] == "no" and (p_j[1] - infect_i[1]) ** 2 + (p_j[2] - infect_i[2]) ** 2 <= 36)) \
                  .map(lambda row: (row[0], 1)) \
                  .reduceByKey(lambda x, y: x + y)

# Show the resulting RDD
close_contacts_rdd.foreach(print)
