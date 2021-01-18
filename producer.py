import time
import json
from kafka.producer import KafkaProducer
import requests_oauthlib
import requests
import re
kafka_boostrap_servers = '127.0.0.1:9092'
kafka_topic_name = 'hashtags'

# Conectar o Producer do kafka
producer = KafkaProducer(bootstrap_servers=kafka_boostrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Substituir valores pelos seus
ACCESS_TOKEN = 'ACCESS_TOKEN'
ACCESS_SECRET = 'ACCESS_SECRET'
CONSUMER_KEY = 'CONSUMER_KEY'
CONSUMER_SECRET = 'CONSUMER_SECRET'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    # Essa é a localização de São Francisco  
    query_data = [('locations', '-122.75,36.8,-121.75,37.8,-74,40,-73,41'), ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    for i in response.iter_lines():
        full_tweet = json.loads(i)
        tweet_text = full_tweet['text'] + '\n'

        # Filtrar as hashtags
        hashtags = re.findall(r'#[a-zA-Z0-9]+', tweet_text, flags=re.IGNORECASE)
        if hashtags:
            for item in hashtags:
                producer.send(kafka_topic_name, item)
        #print("Tweet Text: " + tweet_text)
    # return response


resp = get_tweets()