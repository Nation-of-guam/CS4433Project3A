from pyspark.sql.functions import count, pow, col, when, lit, sum, udf
from pyspark.sql.types import DoubleType, StructType, StructField, IntegerType, StringType
import pyspark as spark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()

# Define the schema of the DataFrame
schema = StructType([
      StructField("id", IntegerType()),
      StructField("x", IntegerType()),
      StructField("y", IntegerType()),
      StructField("INFECTED", StringType())
])

# Load the CSV file into a PySpark DataFrame with the specified schema
people_df = spark.read.format("csv").option("header", True).schema(schema).load("PEOPLE-SOME-INFECTED-large.csv")

# Filter to only include infected people
infected_df = people_df.filter(col("INFECTED") == "yes")

# Define a UDF to compute the Euclidean distance between two points
def distance(x1, y1, x2, y2):
    return pow(x1 - x2, 2) + pow(y1 - y2, 2)

distance_udf = udf(distance)

# Define a suffix to append to column names
suffix = "_1"

# Cross join the infected people DataFrame with itself
pairs_df = infected_df.crossJoin(
    infected_df.select([col(c).alias(c + suffix) for c in infected_df.columns])
)

# Compute the distance between each pair of infected people
pairs_df = pairs_df.withColumn("distance", distance_udf(col("X"), col("Y"), col("X_1"), col("Y_1")))

# Filter to only include pairs of infected people that are within 6 units of each other
pairs_df = pairs_df.filter(pow(col("distance"), 0.5) <= 6.0)


# Group by each infected person and count the number of close contacts
result_df = pairs_df.groupBy("X").agg(count("*").alias("count-of-close-contacts-of-infect-i"))

# Add a column to indicate that the result is for infected people
result_df = result_df.withColumn("INFECTED", lit("yes"))

# Select only the columns we want to return as the final result
result_df = result_df.select("X", "count-of-close-contacts-of-infect-i")

# Show the result
result_df.show()