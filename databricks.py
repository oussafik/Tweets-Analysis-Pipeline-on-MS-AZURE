from http import client
import pymongo
import getpass
import random
import requests
import json
#import time as time_sleep
import tweepy
import time
DB_NAME = "testingcosmos"
COLLECTION_NAME = 'testingdatabrickscosmos'
CONNECTION = "mongodb://testingcosmosdatabricks:qr6dgMN9tXaardkFxMB2IeQSqZuDwdS3jmcimjrS0vC7EnU4jxdJ7ERnFPOikt4PjLU7bQqsv6CGACDbIBD3og==@testingcosmosdatabricks.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@testingcosmosdatabricks@"

api_key = "Qh5Rlstp7jpND3znRWUQ91oSB"
api_secret = "MjOBnsr5Gn30nHnBnEaDH0yxLuEQGPofFhRINtYDMmJ5nTtUTP"
bearer_token = "AAAAAAAAAAAAAAAAAAAAABp1mAEAAAAAy3jWsLhRHvfcTq2xro0S3DlvpyM%3DxsieY8WfieMrJ3pRhQufPjyqDaw5sLbq96JbijgbABcgHgIBH9"
access_token = "1548626209491374080-rOds6Zz1LVrZGrxwa6sor4xHAiaTOi"
access_token_secret = "wO5YuECdwsx2ZYcpgyi8lImMqWNpQJe3ujCqJO3q3fZmq"

auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
api = tweepy.API(auth)
#######start to pull data and run CosmosDB###

class MyStream(tweepy.StreamingClient):
    # This function gets called when the stream is working
    def on_connect(self):
        print("Connected")

    # This function gets called when a tweet passes the stream
    def on_tweet(self, tweet):
        if tweet.referenced_tweets is None:
            # Extracting tweet information
            text = tweet.text
            user_id = tweet.author_id
            retweet_count = tweet.public_metrics['retweet_count']
            created_at = tweet.created_at
            favorite_count = tweet.public_metrics['like_count']
            reply_count = tweet.public_metrics['reply_count']

            # Printing tweet information
            #print(f"Text: {text}\nUser ID: {user_id}\nRetweets: {retweet_count}\nFavorites: {favorite_count}\nReply: {reply_count}\nCreated at: {created_at}\nSentiment: {result}\n")

            message = json.dumps({"User ID":user_id,"Text":text,"Created At":created_at.strftime("%Y-%m-%d %H:%M:%S"),"Retweets":retweet_count,"Favorites":favorite_count,"Reply":reply_count})
            data_ready = json.loads(message)

            ###insert documents into the colleciton####
            def insert_sample_document(client):
                # client = pymongo.MongoClient(connection)
                db = client[DB_NAME]
                """Insert a sample document and return the contents of its id field"""
                collection = db[COLLECTION_NAME]
                document_id = collection.insert_one(data_ready).inserted_id
                print("Inserted document with id {}".format(document_id))
                return document_id

            insert_sample_document(client=pymongo.MongoClient(CONNECTION))
            print("Load to Cosmos Successfull")
            # Delay between tweets
            time.sleep(0.5)

# Creating Stream object
stream = MyStream(bearer_token=bearer_token)

# Adding terms to search rules
for term in ["arsenal","tottenham","new castle","man city","man united","chelsea","liverpool"]:
    stream.add_rules(tweepy.StreamRule(term))

# Starting stream
stream.filter(tweet_fields=["referenced_tweets", "author_id","created_at", "public_metrics","id"])