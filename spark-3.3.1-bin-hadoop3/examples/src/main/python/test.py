from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import time
print("x"*10000)
conf = SparkConf() \
    .setAppName("Example") \
    .setMaster("local") \
    .set("spark.driver.extraClassPath","C:/pyspark/*")#
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

print("y"*10000)

d = {'a':[1,2,3]}

df = spark.read.csv(r"C:\Dev\pyspark\names.csv")

print(df.columns)
time.sleep(10)
