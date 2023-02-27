from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


# Create a SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()

# Define the schema of the CSV files
schema1 = StructType([
      StructField("id", IntegerType()),
      StructField("x", IntegerType()),
      StructField("y", IntegerType())
])
schema2 = StructType([
    StructField("id", IntegerType()),
    StructField("x", IntegerType()),
    StructField("y", IntegerType()),
    StructField("INFECTED", StringType())
])

# Load the PEOPLE-large CSV file as an RDD and extract the (id, x, y) coordinates
people_rdd = spark.read.format("csv").option("header", True).schema(schema1).load("PEOPLE-large.csv") \
    .select("id", "x", "y").rdd.map(tuple)

# Load the INFECTED-small CSV file as an RDD and extract the (id, x, y) coordinates
infected_rdd = spark.read.format("csv").option("header", True).schema(schema2).load("INFECTED-small.csv") \
    .select("id", "x", "y").rdd.map(tuple)

# Compute the join pairs of people who were in close proximity to each infected person
infected_coords = infected_rdd.collect()
join_pairs_rdd = people_rdd.filter(lambda p_j: any((p_j[1] - infect_i[1]) ** 2 + (p_j[2] - infect_i[2]) ** 2 <= 36 for infect_i in infected_coords)) \
                  .map(lambda p_j: (p_j[0], ["Is a close contact" for infect_i in infected_coords if (p_j[1] - infect_i[1]) ** 2 + (p_j[2] - infect_i[2]) ** 2 <= 36]))
join_pairs_rdd = join_pairs_rdd.mapValues(lambda v: list(set(v)))

join_pairs_rdd.foreach(print)