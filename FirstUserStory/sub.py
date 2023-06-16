from google.cloud import pubsub_v1
import json
from google.auth import jwt

topic_name='projects/de2shital/topics/BI_Topic'
subscription_name='projects/de2shital/subscriptions/BI_BQ_Subscriber'

service_account_info = json.load(open("pubsub_BQ_key.json"))
audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"
credentials = jwt.Credentials.from_service_account_info(service_account_info,audience=audience)

def callback(message):
    print(message.data)
    message.ack()

def subscriber(credentials_sub,subscription_name):
    with pubsub_v1.SubscriberClient(credentials=credentials_sub) as subscriber:
        # subscriber.create_subscription(
        #     name=subscription_name, topic=topic_name)
        subscriber.subscribe(subscription_name, callback)
        # try:
        #     future.result()
        # except KeyboardInterrupt:
        #     future.cancel()
