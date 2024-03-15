from pyspark.sql import SparkSession
from pyspark.sql.functions import mean
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Instrument Data Processing") \
    .getOrCreate()# Read the data into a DataFrame

df = spark.read.text("example_input.txt")# Initialize Spark session

df = spark.read.format('csv').option("header", "false").option("inferSchema", "true").load('example_input.txt')
df = spark.read.csv('example_input.txt', header=False, inferSchema=True)
df = spark.read.csv('example_input.txt', header=False, inferSchema=True)

df = spark.read.option("header", False).option("inferSchema", True).csv('example_input.txt')
df = df.withColumnRenamed('_c0', 'Instrument').withColumnRenamed('_c1', 'Date').withColumnRenamed('_c2', 'Value')# Convert 'Date' column to date type

df = df.withColumn('Date', df['Date'].cast('date'))# Calculate mean for INSTRUMENT1
instrument1_mean = \
    df.filter(df['Instrument'] == 'INSTRUMENT1').agg(mean('Value')).collect()[0][0]
# Calculate mean for INSTRUMENT2 for November 2014
instrument2_nov_2014_mean = df.filter((df['Instrument'] == 'INSTRUMENT2') & \
                                      (df['Date'].between('01-11-2024', '30-11-2024'))) \
                              .agg(mean('Value')).collect()[0][0]# Calculate median for INSTRUMENT3

instrument3_median = df.filter(df['Instrument'] == 'INSTRUMENT3') \
                      .agg(F.expr("percentile_approx(Value, 0.5)").alias("median")) \
                      .collect()[0]["median"]

print("Mean for INSTRUMENT1:", instrument1_mean)
print("Mean for INSTRUMENT2 (November 2014):", instrument2_nov_2014_mean)
print("Median for INSTRUMENT3:", instrument3_median)# Stop Spark session
spark.stop()