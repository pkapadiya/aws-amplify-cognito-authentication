from pyspark.context import SparkContext
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import Row,SQLContext
from pyspark.sql.functions import translate,regexp_replace
import sys
# from transforms import *
from utils import getResolvedOptions
# from context import GlueContext
# from dynamicframe import DynamicFrame
from pyspark.sql.functions import udf,translate
from pyspark.sql.types import StringType,IntegerType
from job import Job
import re


replace_special_char=lambda x: (str(x).replace('"',"").replace(",",""))
def f(person):
    # person.name='pankaj' #its read only we cant modify
    print(person.name)



# spark = SparkSession.builder.master("local").appName("voice_cdr").config("spark.some.config.option", "some-value").getOrCreate()
# conf=SparkConf(master="local",appName="voice_cdr")
sc = SparkContext(master="local",appName="voice_cdr")
# sc = SparkContext(conf)
spark = SparkSession(sc)

# sqlContext = SQLContext(sc)
# df = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", ";").load("1.CDR")
# df.show(2)

l = [('Pank"aj', 1),('Binny', 2),('Wahab', 3)]
df = spark.createDataFrame(l, ['name', 'age'])
print("datafram {} and datacount {}".format(df,df.count()))
df.show()

df = sc.parallelize([Row(name='Alice', age=5, height=80), Row(name='Ali"ce', age=5, height=80), Row(name='Alice', age=10, height=80)]).toDF()
print("datafram {} and datacount {}".format(df,df.count()))
df.show()
df.foreach(f)
df=(df.na.replace('Alice','Pankaj')) #replace anystring with new string
df.show()
df.select("age","height",translate("name",'"',"").alias("name")).show() #below will be dynamic
df=df.select(*[translate(column,'"',"").alias(column) for column in df.columns])
print(df)
# df = df.select(*[replace_special_char(column).alias(column) for column in df.columns])
# df.show()
# df=df.collect() #table row format
# print("datafram {} and datacount {}".format(df,df.count()))
# for column in df.columns:
#     print(type(column))

# modDfObj = df.apply(lambda x: x.replace('"',"") if x == 'name' else x)
# print(modDfObj)
# cdr_data_frame = df.select(*[replace_special_char(column).alias(column) for column in df.columns])
# print(cdr_data_frame)
# df.show()
