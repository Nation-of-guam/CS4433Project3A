from pyspark.sql import SparkSession

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate()

df = spark.read.csv("PEOPLE-SOME-INFECTED-large.csv", header=True, inferSchema=True)

infected_count = df.filter(df.INFECTED == 'yes').count()

print("Number of infected people:", infected_count)