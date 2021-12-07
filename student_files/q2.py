import sys
from pyspark.sql import SparkSession
# you may add more import if you need to
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

sc = SparkContext.getOrCreate()


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
spark = SparkSession(sc)

df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

df = df.sort("City",F.col("Price Range").desc(),F.col("Rating"))

windowPR = Window.partitionBy("Price Range").orderBy(col("Rating").desc())
maxdf = df.withColumn("row",row_number().over(windowPR)).filter(col("row") == 1).drop("row").show()

# windowPR = Window.partitionBy("Price Range").orderBy(col("Rating").asc())
# mindf = df.withColumn("row",row_number().over(windowPR)).filter(col("row") == 1).drop("row")

# newdf = maxdf.union(mindf)
# newdf.sort("City",F.col("Price Range").desc(),F.col("Rating")).show()