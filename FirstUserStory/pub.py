from google.cloud import pubsub_v1
import json
from google.auth import jwt
from binance import Client
import pandas as pd
from datetime import datetime
import configparser


config=configparser.ConfigParser()
config.readfp(open(r"config.txt"))

api_key = config.get('BINANCE_API', 'API_KEY')
secret = config.get('BINANCE_API', 'SECRET')



client = Client(api_key, secret)

def get_all_tickers():    
    tickers = client.get_all_tickers()
    #ticker_encode=json.dumps(tickers, ensure_ascii=False).encode('utf8')
    return tickers

#Subscriber
#service_account_info = json.load(open("pubsub_BQ_key.json"))
#audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"
#credentials = jwt.Credentials.from_service_account_info(service_account_info,audience=audience)

def publish(credentials_pub,tickers,topic_name):
    #publisher_audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
    #credentials_pub = jwt.Credentials.from_service_account_info(service_account_info,audience=publisher_audience)
    publisher = pubsub_v1.PublisherClient(credentials=credentials_pub)
    for coin in tickers:
        dt_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        coin["timestamp"]=dt_string
        #print(coin)
        coin_encode=json.dumps(coin, ensure_ascii=False).encode('utf8')
        future = publisher.publish(topic_name, data=coin_encode, spam='eggs')
        future.result()

def callback(message):
    print(message.data)
    message.ack()



