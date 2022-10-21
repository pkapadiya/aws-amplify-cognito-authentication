# we have to add fixed or mobile also along with rates which we can have use in TRA report
# UDF: https://www.bmc.com/blogs/how-to-write-spark-udf-python/
# SQLContext: As of Spark 2.0, this is replaced by SparkSession. However, we are keeping the class here for backward compatibility.
    # A SQLContext can be used create DataFrame, register DataFrame as tables, execute SQL over tables, cache tables, and read parquet files.
# Manual Create Table Structure
row_data_struct = StructType([StructField("Timestamp", StringType()), StructField("StatusType", StringType())])

## Create Data Frame from Files options
# http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=filter#pyspark.sql.DataFrameReader

# Create View from dataframe called telcobridg and query on those data
dateframe.createOrReplaceTempView("telcobridg")
status_null_df = spark.sql("SELECT *,'StatusType is null' as Error from telcobridg where StatusType is null")
status_null_df = spark.sql("SELECT * from telcobridg where StatusType is null")
status_null_df.show()
calling_null_df = spark.sql("SELECT *,'CallingParty is null' as Error from telcobridg where Calling is null")
filter_df = spark.sql("SELECT * from telcobridg where StatusType<>'END'")
filter_df.show()
spark.catalog.dropTempView("telcobridg")


### Spark before version 2.0
# conf=SparkConf(master="local",appName="voice_cdr")
# sc = SparkContext(conf)
# sc = SparkContext(master="local",appName="voice_cdr")
# spark = SparkSession(sc)
# sqlContext = SQLContext(sc)


# Common Methods
replace_special_char=lambda x: (str(x).replace('"',"").replace(",",""))
def f(person):
    # person.name='pankaj' #its read only we cant modify
    print(person.name)    



# tbdf = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", ",").load("telcobridge.cdr")
# tbdf.show()
# startdf=tbdf.filter(tbdf._c1 == 'BEG' or  tbdf._c1 == 'UPD')
# startdf.show()

# tb_fields_mappings=['Timestamp','CallType','SessionId','LegId','StartTime','ConnectedTime','EndTime','Calling','Called','Direction']

# df = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", ";").load("1.CDR")
# df.show(1)

# l = [('Pank"aj', 1),('Binny', 2),('Wahab', 3)]
# df = spark.createDataFrame(l, ['name', 'age'])
# print(f"dataframe {df} and datacount {df.count()}")
# df.show()

# df = sc.parallelize([Row(name='Alice', age=5, height=80), Row(name='Ali"ce', age=5, height=80), Row(name='Alice', age=10, height=80)]).toDF()
# print("datafram {} and datacount {}".format(df,df.count()))
# df.show()
# df.foreach(f)

# df=(df.na.replace('Alice','Pankaj')) #replace anystring with new string
# df.show()

# df.select("age","height",translate("name",'"',"").alias("name")).show() #below will be dynamic
# df=df.select(*[translate(column,'"',"").alias(column) for column in df.columns])
# df.show()
# print(df)
# df = df.select(*[replace_special_char(column).alias(column) for column in df.columns])
# df.show()
# df=df.collect() #table row format
# df.show()
# print("datafram {} and datacount {}".format(df,df.count()))
# for column in df.columns:
#     print(type(column))

# df = sc.parallelize([Row(name='Alice', age=5, height=80), Row(name='Ali"ce', age=5, height=80), Row(name='Alice', age=10, height=80)]).toDF()
# modDfObj = df.apply(lambda x: x.replace('"',"") if x == 'name' else x)
# modDfObj.show()
# print(modDfObj)
# cdr_data_frame = df.select(*[replace_special_char(column).alias(column) for column in df.columns])
# print(cdr_data_frame)
# df.show()

