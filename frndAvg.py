from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

f = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///Users/sk/Desktop/LEARNING/SparkCourse/fakefriends-header.csv")
    
frndA= f.select("age","friends")

frndA.groupby("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show()

spark.stop()