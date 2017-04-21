#A generic python program to download tweets from
#Twitter and using Twitter API with the aid of the 
#library Tweepy and save it to mongoDB
#Do not forget to download using pip the Tweepy and pymongo libraries

from httplib import IncompleteRead # Python 2
import sys
import json
import time
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from pymongo import MongoClient

#Variables that contains the user credentials to access Twitter API
access_token = "***********************************"
access_token_secret = "***********************************"
consumer_key = "***********************************"
consumer_secret = "***********************************"

#connect to mongoDB
client = MongoClient("127.0.0.1:27017")
db = client.dbTweets
coll = db['YourDBCollectionNameHere']

#start timer
startTime = time.time()

#keyword to filter from twitter
keyword = ['Music']

#This is a basic listener that just prints received tweets to stdout.
class MyStreamListener(StreamListener):

    def __init__(self, api=None):
        self.counter = 0
        self.limitDays = 3


    def on_data(self, data):

        # Decode JSON
        datajson = json.loads(data)

        try:
            # We only want to store tweets in English
            if "lang" in datajson and "text" in datajson and datajson["lang"] == "en":

                #check if tweet's text contains the keyword
                if any(elem in datajson["text"].encode("utf-8") for elem in keyword):

                    # Store tweet info into the collection.
                    coll.insert(datajson)

                    #increase the tweets counter by one
                    self.counter = self.counter + 1

                    #print text only
                    print self.counter, ") ", datajson["text"].encode("utf-8")
                    print "\n\n"

        except KeyError:
            #catch any Key encode error from json document
            print "Print Key Error"

        #check time for termination conditions
        if (time.time() - startTime) >= (self.limitDays * 24 * 60 * 60):
            client.close()
            print "elapsed time in minutes", (time.time() - startTime)/60
            sys.exit(0)


    def on_error(self, status_code):
        print("status code", status_code)
        return True # Don't kill the stream


    def on_timeout(self):
        return True # Don't kill the stream



if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = MyStreamListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    while True:
        try:
            stream = Stream(auth, l)

            #This line filter Twitter Streams to capture data by the keyword-s
            stream.filter(track=keyword, stall_warnings=True)

        except IncompleteRead:
            pass
            # Oh well, reconnect and keep trucking

        except KeyboardInterrupt:
            stream.close()
            sys.exit(0)
