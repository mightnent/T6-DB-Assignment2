from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()

spark = SparkSession(sc)
hdfs_nn = "172.31.20.157"
df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
df.printSchema()
df.show()
