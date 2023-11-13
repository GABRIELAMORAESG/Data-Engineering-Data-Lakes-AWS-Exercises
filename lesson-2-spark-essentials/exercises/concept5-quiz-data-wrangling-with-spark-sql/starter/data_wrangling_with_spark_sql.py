#!/usr/bin/env python
# coding: utf-8

# # Data Wrangling with Spark SQL Quiz
# 
# This code uses the same dataset and most of the same questions from the earlier code using dataframes. For this scropt, however, use Spark SQL instead of Spark Data Frames.
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "data/sparkify_log_small.json"
# 4) create a view to use with your SQL queries
# 5) write code to answer the quiz questions 

# Iniciating Spark Session
spark = SparkSession.builder.appName("Data wrangling with Spark SQL").getOrCreate()

#File path
path = "./lesson-2-spark-essentials/exercises/data/sparkify_log_small_2.json"
sparkify_log_df = spark.read.json(path)

# Print schema 
sparkify_log_df.printSchema()

# sample of database
sparkify_log_df.show(5)

#Create a temp table or view
sparkify_log_df.createOrReplaceTempView("sparkify_log_df")

# # Question 1
# Which page did user id ""(empty string) NOT visit?


# TODO: write your code to answer question 1
q1 = spark.sql('''
            SELECT 
                Page
          
            FROM sparkify_log_df
            WHERE Page not in (SELECT page FROM sparkify_log_df WHERE userId = "" GROUP BY 1)
            GROUP BY 1
          
          ''')

q1.show()


# # Question 2 - Reflect
# Why might you prefer to use SQL over data frames? Why might you prefer data frames over SQL?


# # Question 3 
# How many female users do we have in the data set?

# TODO: write your code to answer question 3
q3 = spark.sql('''
            SELECT 
                gender
                , count(Distinct userId) as distinct_users
          
            FROM sparkify_log_df
            GROUP BY 1
          ''')
q3.show()

# # Question 4
# How many songs were played from the most played artist?

# TODO: write your code to answer question 4
q4 = spark.sql('''
            SELECT 
                artist
                , count(*) as song_played
          
            FROM sparkify_log_df
            WHERE artist is not null
               
            GROUP BY 1
            
               ORDER BY 2 DESC 
               LIMIT 1
          ''')
q4.show()


# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# TODO: write your code to answer question 5
q5 = spark.sql('''
            SELECT 
                artist
                , count(*) as song_played
          
            FROM sparkify_log_df
            WHERE artist is not null
               
            GROUP BY 1
            
               ORDER BY 2 DESC 
               LIMIT 1
          ''')
q5.show()



