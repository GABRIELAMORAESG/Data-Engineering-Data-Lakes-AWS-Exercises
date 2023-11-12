from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import *
from pyspark.sql.window import Window

#Spark Context
spark = SparkSession \
    .builder \
    .appName("Wrangling Data") \
    .getOrCreate()

# Reading data from the path 
path = "./lesson-2-spark-essentials/exercises/data/sparkify_log_small.json"
sparkify_log_small_df = spark.read.json(path)

# # Calculate Statistics by Hour
get_hour = udf(lambda x: datetime.fromtimestamp(x / 1000.0). hour)
sparkify_df = sparkify_log_small_df.withColumn("hour", get_hour(sparkify_log_small_df.ts))
sparkify_df.show(5)

# Which page did user id "" (empty string) NOT visit?
empty_string_user = sparkify_df.filter(col("userId") == "")
page_es = empty_string_user.select("page").dropDuplicates()

page_es.show()

# How many female users do we have in the data set?
df2 = (sparkify_df
       .select("userId", "gender")
       .dropDuplicates()
       .groupby("gender")
       .count()
)

df2.show()

#How many songs were played from the most played artist?
df3 = (sparkify_df
       .filter(col("artist") != "")
       .groupBy("artist").count().sort(desc("count"))
)
df3.show(1)

# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
user_window = Window \
    .partitionBy('userID') \
    .orderBy(desc('ts')) \
    .rangeBetween(Window.unboundedPreceding, 0)

ishome = udf(lambda ishome : int(ishome == 'Home'), IntegerType())

# Filter only NextSong and Home pages, add 1 for each time they visit Home
# Adding a column called period which is a specific interval between Home visits
cusum = sparkify_df.filter((sparkify_df.page == 'NextSong') | (sparkify_df.page == 'Home')) \
    .select('userID', 'page', 'ts') \
    .withColumn('homevisit', ishome(col('page'))) \
    .withColumn('period', sum('homevisit') \
    .over(user_window)) 
    
# This will only show 'Home' in the first several rows due to default sorting

#cusum.show(300)

cusum.filter((cusum.page == 'NextSong')) \
    .groupBy('userID', 'period') \
    .agg({'period':'count'}) \
    .agg({'count(period)':'avg'}) \
    .show()

