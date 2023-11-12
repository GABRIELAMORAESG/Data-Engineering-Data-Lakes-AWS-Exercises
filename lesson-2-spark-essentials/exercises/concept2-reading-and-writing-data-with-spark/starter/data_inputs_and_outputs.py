import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()

# This should print the default configuration
#print(spark.sparkContext.getConf().getAll())   

# This path resides on your computer or workspace, not in HDFS
path = "./lesson-2-spark-essentials/exercises/data/sparkify_log_small_2.json"
log_small_df = spark.read.json(path)

log_small_df.printSchema()

log_small_df.show(n=1)

d = log_small_df.describe().show()

t = log_small_df.take(5)
print(t)


out_path = "../../data/sparkify_log_small.csv"
log_small_df.write.mode("overwrite").save(out_path, format="csv", header=True)

user_log_2 = spark.read.csv(out_path, header=True)

user_log_2.printSchema()

d = user_log_2.describe().show()

t = user_log_2.take(5)
print(t)
