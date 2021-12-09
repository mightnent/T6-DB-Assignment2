import sys
from pyspark.sql import SparkSession
# you may add more import if you need to
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import asc,desc,col

sc = SparkContext.getOrCreate()


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
spark = SparkSession(sc)

df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

df = df.na.drop(subset=["Rating"])
df = df.na.drop(subset=["Price Range"])

df = df.withColumn('Rating',df['Rating'].cast("float").alias('Rating'))

df.printSchema()

df1 = df.groupBy('City','Price Range').agg(F.max('Rating').alias('Rating'))
df2 = df.groupBy('City','Price Range').agg(F.min('Rating').alias('Rating'))

df_u = df1.union(df2)

df_join = df.join(df_u,on=["City","Price Range","Rating"])
new_df = df_join.sort(asc("City")).dropDuplicates(subset=["City","Price Range","Rating"])

new_df.show()

new_df.write.csv("hdfs://%s:9000/assignment2/output/question2/" % (hdfs_nn), header=True)





