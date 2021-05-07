# Bigdata_casestudy
this project is dealing with streaming data from twitter API ,fetching this data using tweepy package and sending it to kafka topic.
spark streaming will consume the data to apply sentiment analysis on tweets to specify the negative and positive feedbacks, based on this analysis the application will reply to the user account who posted the feedback ,all of this will be in real time.
then the data will be stored in parquet file format on hdfs to build hive table and use the data for bilding dashboard using power BI for deep analysis.

# versions used for building this project:


hortonwork HDP 2.6
python == 3.6
kafka == 2.11
spark == 2.4.6
pyspark == 2.4.6
power BI desktop
