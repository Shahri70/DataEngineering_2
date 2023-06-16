import json
from google.auth import jwt
from binance import Client
import sub as Subscriber
import pub as Publisher
import configparser


config=configparser.ConfigParser()
config.readfp(open(r"config.txt"))

#GCP credentials
topic_name=config.get('GCP', 'TOPIC_NAME')
subscription_name=config.get('GCP', 'SUBSCRIBER_NAME')
api_key = config.get('BINANCE_API', 'API_KEY')
secret = config.get('BINANCE_API', 'SECRET')

# client = Client(api_key, secret)

# tickers = client.get_all_tickers()
# ticker_encode=json.dumps(tickers, ensure_ascii=False).encode('utf8')

service_account_info = json.load(open("pubsub_BQ_key.json"))
audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"
publisher_audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
credentials_sub = jwt.Credentials.from_service_account_info(service_account_info,audience=audience)
credentials_pub = jwt.Credentials.from_service_account_info(service_account_info,audience=publisher_audience)

if __name__ == "__main__":
    tickerdata=Publisher.get_all_tickers()
    Publisher.publish(credentials_pub,tickerdata,topic_name)
    Subscriber.subscriber(credentials_sub,subscription_name)


