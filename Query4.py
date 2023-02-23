from pyspark.sql import SparkSession
from pyspark.sql.functions import sqrt

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()

people = spark.read.csv("PEOPLE-large.csv", header=True, inferSchema=True)

infected = spark.read.csv("INFECTED-small.csv", header=True, inferSchema=True)

join_pairs = infected.join(people, how='cross') \
                    .filter(sqrt((infected.x - people.x) ** 2 + (infected.y - people.y) ** 2) <= 6) \
                    .select(people.id.alias('pi'))

unique_join_pairs = join_pairs.distinct()

unique_join_pairs.show()
