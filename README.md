# Bigdata_casestudy
this project is dealing with streaming data from twitter API ,fetching this data using tweepy package and sending it to kafka topic.
spark streaming will consume the data to apply sentiment analysis on tweets to specify the negative and positive feedbacks, based on this analysis the application will reply to the user account who posted the feedback ,all of this will be in real time.
then the data will be stored in parquet file format on hdfs to build hive table and use the data for bilding dashboard using power BI for deep analysis.
## packages used :
tweepy is the package used for fetching data from twitter API , streamListener to listen to data in real time.
using KafkaProducer to send data to kafka topic named twitter .
spark session and spark context to use structured stream for reading data from kafka topic to spark for analysis.
textblob is the function used for sentiment analysis.
### starting commands:
#creating kafka topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --repartition-factor 1 --partitions 1 --topic <name>
##starting working with spark:
 spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 /scriptpath.py

# versions used for building this project:

install pykafka
hortonwork HDP 2.6
python == 3.6
kafka == 2.11
spark == 2.4.6
pyspark == 2.4.6
power BI desktop
