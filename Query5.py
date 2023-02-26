from pyspark.sql.functions import count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from math import sqrt

spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()

# Define the function to calculate distance between two points
# Returns:
# INFECTED id, nonInfected id, distance
def funct(x):
    return (x[1][0],x[0][0], sqrt(((x[0][1]-x[1][1])*(x[0][1]-x[1][1]))+((x[0][2]-x[1][2])*(x[0][2]-x[1][2]))))

# Define the schema of the DataFrame
schema = StructType([
      StructField("id", IntegerType()),
      StructField("x", IntegerType()),
      StructField("y", IntegerType()),
      StructField("INFECTED", StringType())
])

# Load the CSV file into a PySpark DataFrame with the specified schema and then change to an rdd
people_rdd = spark.read.format("csv").option("header", True).schema(schema).load("PEOPLE-SOME-INFECTED-large.csv").rdd

# Filter the infected people from the parsed data
infected_people = people_rdd.filter(lambda person: person[3] == "yes").collect()
not_infected = people_rdd.filter(lambda person: person[3] == "no")

# Cross-join the parsed data with the infected people
joined_data = not_infected.flatMap(lambda row: [(row, person) for person in infected_people])

# Compute the distance between each person and the infected people
distances = joined_data.map(lambda row: funct(row) if funct(row)[2] <= 6 else (-1,-1,100))

#Filters to just ids with distance of 6 or less
filtered_distances = distances.filter(lambda row: row[0] != -1).sortBy(lambda x : x[0])

result = filtered_distances.map(lambda x : (x[0], x[1])).groupByKey().mapValues(lambda v : len(set(v))).collect()


for i in result:
    print(i)