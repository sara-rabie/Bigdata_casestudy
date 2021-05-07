import logging
import parquet
import json
from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import tweepy as tw

##create function for sentiment analysis:
@psf.udf
def getSentiment(tweet):
    blob_object = TextBlob(tweet, analyzer=NaiveBayesAnalyzer())
    if blob_object.polarity > 0 :
      return "positive"
    elif blob_object.polarity < 0:
      return 'negative'
            
    else:
      return 'neutral'  
      
#return reply based on sentiment analysis:
  
def reply_to_tweet(tweet):
    user = tweet.user_name
    if tweet.sentiment=="positive":
      msg = "@%s Thank you for ! Keep it up!" %user
    else:
      msg = "@%s We're sorry you're feeling that way!" %user
    msg_sent = api.update_status(msg, tweet.tweet_id)
    print("################Reply Posted###############")
  	
   

     


logger = logging.getLogger(__name__)

schema = StructType([
	StructField("user_name", StringType(), True),
	StructField("text", StringType(), True),
	StructField("tweet_id", StringType(), True),
  StructField("timestamp", StringType(), True),
  StructField("location", StringType(), True),
  StructField("followers_count", IntegerType(), True),
  StructField("friends_count", IntegerType(), True),
  StructField("retweet_count", IntegerType(), True),
  StructField("reply_count", IntegerType(), True),
  StructField("senti_val", DoubleType(), True) 
   
])

consumer_key = "qZI8IAXkHA8mYMqUxWzlrv5Cj"
consumer_secret = "l6xrZ7izF8QUBDCblJ8Jvt6sOdDo21WhKbWEeJV1vZ41SxkjkW"
access_key = "1385705262842056707-ZhnJj2v8o8yLzCKqew0EPjkrZTLXIp"
access_secret = "6pV0JdXqzU1YAYXFyUEBIYnlwJSKgNZRMu1sxeKmt1VK0"



auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tw.API(auth)

spark = SparkSession \
	.builder \
	.master("local[1]") \
	.config('spark.ui.port', '3000') \
	.appName("TwitterSentiment-41") \
	.getOrCreate()

spark.conf.set("spark.sql.streaming.checkpointLocation", "./ck/")
spark.sparkContext.setLogLevel('WARN')


getSentimentUDF = psf.udf(getSentiment, StringType())

df = spark.readStream.\
	format("kafka").\
	option("kafka.bootstrap.servers", "172.18.0.2:6667").\
	option("subscribe","twitter").\
	option("spark.streaming.kafka.consumer.cache.enabled", "false").\
	option("startingOffsets", "latest").\
	option("maxOffsetPerTrigger", "1000").\
	load()

df.printSchema()

kafka_df = df.selectExpr("CAST (value as STRING)")

service_table = kafka_df\
    	.select(psf.from_json(psf.col('value'), schema).alias("Data"))\
    	.select("Data.*")


sentimentData = service_table.withColumn('sentiment', getSentiment(service_table.text))


query3 = sentimentData.writeStream\
	.outputMode("append")\
	.format("console")\
	.start()
reply_query = sentimentData.writeStream.foreach(reply_to_tweet).start()

query = sentimentData.writeStream.format("parquet").\
option("path","hdfs://172.18.0.2:8020/apps/hive/warehouse/case_study.db/tweets/").\
option("checkpointLocation","hdfs://172.18.0.2:8020/apps/hive/warehouse/case_study.db/check").start()

#query = sentimentData.writeStream.format("parquet").outputMode("append") \
#.option("path", "hdfs://172.18.0.2:8020/apps/hive/warehouse/case_study.db/tweets/") \
#.option("checkpointLocation", "hdfs://172.18.0.2:8020/user/root/case_study/check/").start()
query3.awaitTermination()

logger.info("Spark started")

query.awaitTermination()

reply_query.awaitTermination()
#query2.awaitTermination()



