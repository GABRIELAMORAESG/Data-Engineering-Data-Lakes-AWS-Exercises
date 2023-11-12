from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import *
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

#Spark Context
spark = SparkSession \
    .builder \
    .appName("Wrangling Data") \
    .getOrCreate()

# Reading data from the path 
path = "./lesson-2-spark-essentials/exercises/data/sparkify_log_small.json"
sparkify_log_small_df = spark.read.json(path)

# # Data Exploration 
# # Explore the data set.

# View 5 records 
sparkify_log_small_df.show(5)

# Print the schema
sparkify_log_small_df.printSchema()

# Describe the dataframe
#sparkify_log_small_df.describe().show()

# Describe the statistics for the song length column
sparkify_log_small_df.describe("length").show()

# Count the rows in the dataframe
cs = sparkify_log_small_df.count()
print(cs)
   

# Select the page column, drop the duplicates, and sort by page
pc = sparkify_log_small_df.select("page").dropDuplicates().sort("page")
pc.show()

# Select data for all pages where userId is 1046
pc2 = (sparkify_log_small_df
      .filter(col("userId")== 1046)
      .select("userId", "firstname", "page", "song")
)
pc2.show()

# # Calculate Statistics by Hour
get_hour = udf(lambda x: datetime.fromtimestamp(x / 1000.0). hour)

sparkify_df = sparkify_log_small_df.withColumn("hour", get_hour(sparkify_log_small_df.ts))
sparkify_df.show(5)

# Select just the NextSong page
next_song = (sparkify_df
             .filter(col("page")== "NextSong")
             .groupby("hour")
             .count()
             .orderBy(sparkify_df.hour.cast("float"))
)

next_song.show()

# ploting values
next_song.select("hour", "count").toPandas().plot.scatter(x="hour", y="count")

plt.xlim(-1, 24)
plt.ylim(0, 1.2 * next_song.agg({"count": "max"}).collect()[0][0])
plt.xlabel("Hour")
plt.ylabel("Songs Played")
plt.show()

# # Drop Rows with Missing Values
sparkify_df_clean = sparkify_df.dropna(how = "any", subset = ["userId", "sessionId"])

# How many are there now that we dropped rows with null userId or sessionId?
print(sparkify_df.count())
print(sparkify_df_clean.count())

# select all unique user ids into a dataframe
user = (sparkify_df_clean
        .select("userId")
        .dropDuplicates()
        .sort("userId")
)
user.show()

# Select only data for where the userId column isn't an empty string (different from null)
user_log_valid_df = sparkify_df_clean.filter(col("userId") != "")
print(user_log_valid_df.count())

# # Users Downgrade Their Accounts

# Find when users downgrade their accounts and then show those log entries. 
user_log_df = user_log_valid_df.filter(col("page") == "Submit Downgrade")
user_log_df.select(["userId", "firstname", "page", "level", "song"]) \
    .where(user_log_df.userId == "1138") \
    .show()

 
# Create a user defined function to return a 1 if the record contains a downgrade
downgrade = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())

# Select data including the user defined function
user_log_valid_df = user_log_valid_df.withColumn( "downgraded", downgrade("page"))
user_log_valid_df.show()

# Partition by user id
from pyspark.sql import Window
# Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.
windowval = Window.partitionBy("userId") \
    .orderBy(desc("ts")) \
    .rangeBetween(Window.unboundedPreceding, 0)



# Fsum is a cumulative sum over a window - in this case a window showing all events for a user
# Add a column called phase, 0 if the user hasn't downgraded yet, 1 if they have
user_log_valid_df = user_log_valid_df \
    .withColumn("phase", sum("downgraded").over(windowval))



# Show the phases for user 1138 
user_log_valid_df.show()    
user_log_valid_df \
    .select(["userId", "firstname", "ts", "page", "level", "phase"]) \
    .where(user_log_df.userId == "1138") \
    .sort("ts") \
    .show()

