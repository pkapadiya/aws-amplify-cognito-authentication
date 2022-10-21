from pyspark.context import SparkContext
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, ArrayType
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import Row,SQLContext
from pyspark.sql.functions import translate,regexp_replace, lit, udf, col, max, array, broadcast
import sys
import re
from datetime import datetime

## Methods
get_calling_number = udf(lambda x: (re.sub(r'^(\d*#)?([0+]*)?(973)?(\d*)', r'\g<4>',x)), StringType())
get_called_number = udf(lambda x: (re.sub(r'^(\d*#)?([0+]*)?(973)?(\d*)', r'\g<4>',x)), StringType())
get_rates = udf(lambda x: (re.sub(r'^(\d*#)?([0+]*)?(973)?(\d*)', r'\g<4>',x)), StringType())

### Spark After version 2.0 ###
spark = SparkSession.builder.master("local").appName("voice_cdr").config("spark.some.config.option", "some-value").getOrCreate()
sc=spark.sparkContext
### Spark before version 2.0
# sc = SparkContext(master="local",appName="voice_cdr")
# sqlContext = SQLContext(sc)

source_fileds_mapping=['timestamp','status_type','session_id','start_time','end_time','calling_party_id','called_party_id','duration','direction']

row_data_struct = StructType([*[StructField(column_name,StringType()) for column_name in source_fileds_mapping]])

ratecard_struct = StructType([StructField('rate',StringType()),StructField('prefix',StringType()),StructField('pulse',StringType()),StructField('UOC',StringType())])

destination_struct = StructType([StructField('prefix',StringType()),StructField('name',StringType()),StructField('destination_name',StringType())])


dateframe=spark.read.csv('telcobridge.cdr',sep=',',header=False,schema=row_data_struct).dropDuplicates()
ratecard=spark.read.csv('rate-card-1.csv',sep=',',header=False,schema=ratecard_struct).dropDuplicates()
destination=spark.read.csv('prefix.csv',sep=',',header=False,schema=destination_struct).dropDuplicates()

# b = [(1,(1, 4)), (2, (2, 5)), (3, (4,5))]
broadcast(ratecard)
broadcast(destination)
# print(rdd_b.value[0])

# print(ratecard.collectAsMap())

# print("##### CDR DataFrame")
# dateframe.show()
# print("##### CDR RateCard")
# ratecard.show()
# print("##### CDR destination")
# destination.show()

start_time=datetime.now()

def get_rate_details(called_number):
    # print(type(called_number))
    # print(called_number)
    # query="SELECT mr.rate,mr.pulse,cnt.name,cnt.destination_name,mr.UOC FROM destination cnt,ratecard mr WHERE cnt.prefix = mr.prefix and cnt.prefix ='35'"
    # print(query)
    # rates_df=spark.sql(query)
    # rates_df=spark.sql("SELECT mr.rate,mr.pulse,cnt.name,cnt.destination_name,mr.UOC FROM destination cnt,ratecard mr WHERE cnt.prefix = mr.prefix and cnt.prefix = (select max(prefix) prefix from(select cnt.prefix, '"+called_number+"' as call_number from destination cnt,ratecard mr where cnt.prefix = mr.prefix and '"+called_number+"' like concat(cnt.prefix, '%')) contr group by call_number)") 
    # rates_df.show()
    # stopWords = self.stopWordsBC
    # return tokenizer(array(rates_df["rate"],rates_df["pulse"],rates_df["name"],rates_df["destination_name"],rates_df["UOC"]),stopWords)
    # print(type(rates_df["rate"]))
    return "pankaj"
    # return rates_df


def toIntEmployee(rdd):
    e='pankaj'
    query="SELECT mr.rate,mr.pulse,cnt.name,cnt.destination_name,mr.UOC FROM destination cnt,ratecard mr WHERE cnt.prefix = mr.prefix and cnt.prefix ='35'"
    rates_df=spark.sql(query)
    # print(type(rdd["destination_name"]))
    # return Row(e)
    return Row(rdd["prefix"],rdd["name"],rdd["destination_name"],e)

def create_rating_dataframe(rdd): 
    # ratedf=get_rate_details(rdd["called_party_id"])
    # ratedf.show()
    query="SELECT mr.rate,mr.pulse,cnt.name,cnt.destination_name,mr.UOC FROM destination cnt,ratecard mr WHERE cnt.prefix = mr.prefix and cnt.prefix ='35'"
    rates_df=spark.sql(query)
    # df12=destination.where(col('prefix')=='35')
    return Row(rdd["timestamp"],rdd["status_type"],rdd["session_id"],rdd["start_time"],rdd["end_time"],rdd["calling_party_id"],rdd["called_party_id"],rdd["duration"],rdd["direction"])

## Test
dateframe.createOrReplaceTempView("telcobridg")
ratecard.createOrReplaceTempView("ratecard")
destination.createOrReplaceTempView("destination")

# rdd=destination.rdd.map(toIntEmployee)
# rdd=dateframe.rdd.map(create_rating_dataframe)
# spark.createDataFrame(rdd).show()

df=spark.range(10)
df.show()

# sqlContext.cacheTable('destination')
# sqlContext.cacheTable('ratecard')

# calling_null_df = spark.sql("SELECT * from destination a,ratecard b where a.prefix=b.prefix")
# calling_null_df = spark.sql("SELECT mr.rate,mr.pulse,cnt.name,cnt.destination_name,mr.UOC FROM destination cnt,ratecard mr WHERE cnt.prefix = mr.prefix and cnt.prefix = (select max(prefix) prefix from(select cnt.prefix, '35108646' as call_number from destination cnt,ratecard mr where cnt.prefix = mr.prefix and '35108646' like concat(cnt.prefix, '%')) contr group by call_number)")
# calling_null_df.show()

# ratecard.join(destination,ratecard.prefix==destination.prefix).select(ratecard.rate,ratecard.pulse,destination.destination_name,ratecard.UOC).show()

# call_number='35108646'
# pknumber = udf(lambda x: call_number, StringType())
# print(pknumber(call_number))
# ratecard.join(destination,ratecard.prefix==destination.prefix).select(ratecard.rate,pknumber(destination.prefix).alias('call_number'),ratecard.pulse,destination.destination_name,ratecard.UOC).where(col('call_number').like("35%")).show()

# ratecard.join(destination,ratecard.prefix==destination.prefix).select(ratecard.rate,pknumber(destination.prefix).alias('call_number'),ratecard.pulse,destination.destination_name,ratecard.UOC).where(destination.prefix==ratecard.prefix).show()

# ratecard.join(destination,ratecard.prefix==destination.prefix).select(ratecard.rate,pknumber(destination.prefix).alias('call_number'),destination.prefix,ratecard.pulse,destination.destination_name,ratecard.UOC).where(destination.prefix==(destination1.select('prefix').where(col('prefix')=='35').prefix)).show()

# ratecard.join(destination,ratecard.prefix==destination.prefix).select(ratecard.rate,pknumber(destination.prefix).alias('call_number'),ratecard.pulse,destination.destination_name,ratecard.UOC).where(destination.prefix=='35').show()

# print(destination.where(col('prefix')=='35').prefix)

# ratecard.join(destination,ratecard.prefix==destination.prefix).select(ratecard.rate,pknumber(destination.prefix).alias('call_number'),destination.prefix,ratecard.pulse,destination.destination_name,ratecard.UOC).where("call_number like prefix||'%'").groupBy('call_number').agg(max('prefix').alias('prefix')).select('prefix').prefix


# print(type(ratecard.join(destination,ratecard.prefix==destination.prefix).select(ratecard.rate,pknumber(destination.prefix).alias('call_number'),destination.prefix,ratecard.pulse,destination.destination_name,ratecard.UOC).where("call_number like prefix||'%'").groupBy('call_number').agg(max('prefix').alias('prefix')).select('prefix').first()))

# spark.sql("SELECT mr.rate,mr.pulse,cnt.name,cnt.destination_name,mr.UOC FROM destination cnt,ratecard mr WHERE cnt.prefix = mr.prefix").show()

# (select cnt.prefix, '"+called_number+"' as call_number from destination cnt,ratecard mr where cnt.prefix = mr.prefix and '"+called_number+"' like concat(cnt.prefix, '%'))

# print(type(array('pankaj','pankaj')))
def test_function():
    return array('pankaj','pankaj')     

# rate=get_rate_details(dateframe["called_party_id"])

# schema = StructType([
#     StructField("rate", IntegerType(), False),
#     StructField("pulse", IntegerType(), False),
#     StructField("name", StringType(), False),
#     StructField("destination_name", StringType(), False),
#     StructField("UOC", IntegerType(), False),
# ])
# print(get_rate_details('35108646'))
# get_rate_details_udf = udf(get_rate_details, schema)

# get_rate_details_udf = udf(get_rate_details, ArrayType(StringType()))
# get_rate_details_udf = udf(get_rate_details, StringType())
# get_rate_details_udf = udf(lambda z: get_rate_details(z), StringType())
# spark.udf.register("get_rate_details_udf", get_rate_details_udf)
# dateframe1=dateframe.withColumn("rates", get_rate_details_udf("called_party_id"))
# dateframe1.show()

# # sqlContext.clearCache()
# spark.catalog.dropTempView("telcobridg")
# spark.catalog.dropTempView("ratecard")
# spark.catalog.dropTempView("destination")


## Test

## Processing
## Validation Rules Before Filter
# print('status_type is Null')
# status_null_df = dateframe.filter('status_type is Null')
# dateframe=dateframe.subtract(status_null_df)

## Filter Rules
# print('status_type not END')
# not_endcalls_df = dateframe.filter("status_type<>'END'")
# dateframe=dateframe.subtract(not_endcalls_df)
# print('duration<0')
# duration_df = dateframe.filter("duration<=0")
# dateframe=dateframe.subtract(duration_df)

## Validation Rules After Filter
# print('Calling is Null')
# calling_null_df = dateframe.filter('calling_party_id is Null')
# dateframe=dateframe.subtract(calling_null_df)
# print('called_party_id is Null')
# called_null_df = dateframe.filter('called_party_id is Null')
# dateframe=dateframe.subtract(called_null_df)
# print('called_party_id is Null')
# duration_null_df = dateframe.filter('duration is Null')
# dateframe=dateframe.subtract(duration_null_df)
# error_df=status_null_df.withColumn("Error",lit('status_type is null')).unionAll(calling_party_id_null_df.withColumn("Error",lit('calling_party_id is null'))).unionAll(called_null_df.withColumn("Error",lit('called_party_id is null'))).unionAll(duration_null_df.withColumn("Error",lit('duration is null')))
# print("##### Error Records")
# error_df.show()

# filter_df= not_endcalls_df.withColumn("Filter",lit('CallType is Not EndCall')).unionAll(duration_df.withColumn("Filter",lit('duration is less or equals 0')))
# print("##### Filter Records")
# filter_df.show()

## Enrichment on Data
# dateframe=dateframe.withColumn("calling_party_id", get_calling_number(dateframe["calling_party_id"])).withColumn("called_party_id", get_called_number(dateframe["called_party_id"]))
# dateframe.show()

end_time=datetime.now()
print(end_time-start_time)
