import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
import pykafka
from afinn import Afinn
from time import sleep

class TweetListener(StreamListener):
	def __init__(self):
		self.client = pykafka.KafkaClient("172.18.0.2:6667")
		self.producer = self.client.topics[bytes('twitter','ascii')].get_producer()

	def on_data(self, data):
		try:
			json_data = json.loads(data)

			send_data = '{}'
			json_send_data = json.loads(send_data)
			
      
			json_send_data['user_name'] = json_data['user']['screen_name']			
			json_send_data['text'] = json_data['text']
			json_send_data['tweet_id'] = json_data['id']
			json_send_data['timestamp'] = json_data['created_at']
			json_send_data['location'] = json_data['user']['location']
			#json_send_data['country_name'] = json_data['place']
			json_send_data['followers_count'] = json_data['user']['followers_count']
			json_send_data['friends_count'] = json_data['user']['friends_count']
			json_send_data['retweet_count'] = json_data['retweet_count']
			json_send_data['reply_count'] = json_data['reply_count']
			json_send_data['senti_val']=afinn.score(json_data['text'])
      
      
       

			

			self.producer.produce(bytes(json.dumps(json_send_data),'ascii'))
			#self.producer.produce(bytes(json_data),'ascii')
			return True
		except KeyError:
			return True

	def on_error(self, status):
		print(status)
		return True

if __name__ == "__main__":
	
	
	

	consumer_key = "qZI8IAXkHA8mYMqUxWzlrv5Cj"
	consumer_secret = "l6xrZ7izF8QUBDCblJ8Jvt6sOdDo21WhKbWEeJV1vZ41SxkjkW"
	access_token = "1385705262842056707-ZhnJj2v8o8yLzCKqew0EPjkrZTLXIp"
	access_secret = "6pV0JdXqzU1YAYXFyUEBIYnlwJSKgNZRMu1sxeKmt1VK0"

	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_secret)
	
	# create AFINN object for sentiment analysis
	afinn = Afinn()

	twitter_stream = Stream(auth, TweetListener())
	twitter_stream.filter(languages=['en'], track=['covid'])
  

      
       
 
sleep(3000)