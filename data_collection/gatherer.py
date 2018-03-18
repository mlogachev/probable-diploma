import tweepy
import pymongo
import time

from api.tweepy_api import api

WORDS_LIST = [
    'и',
    'в',
    'не',
    'он',
    'на',
    'я',
    'что',
    'тот',
    'был',
    'быть',
    'будет',
    'с',
    'я',
    'весь',
    'все',
    'это',
    'этот',
    'как',
    'она',
    'по',
    'но',
    'они',
    'к',
    'у',
    'ты',
    'твой',
    'их',
    'из',
    'мы',
    'вы',
    'так',
    'же',
    'от',
    'сказал',
    'сказать',
    'который',
    'человек',
    'может',
    'мочь',
    'мог',
    'смог',
    'сможет',
    'кто',
    'для',
    'вот',
    'да',
    'год',
    'человек',
    'люди',
]


class MyStreamListener(tweepy.StreamListener):

    def __init__(self, mongo_collection):
        super().__init__()
        self.collection = mongo_collection

    def on_status(self, status):
        if 'retweeted_status' in status._json:

            status = status._json
            retweetd = status['retweeted_status']

            tweet = dict(
                created_at=retweetd['created_at'],
                id=retweetd['id_str'],
                quotes=retweetd['quote_count'],
                replies=retweetd['reply_count'],
                retweets=retweetd['retweet_count'],
                favorites=retweetd['favorite_count']
            )

            try:
                text = retweetd['extended_tweet'][
                    'full_text']
            except KeyError:
                print("NO EXTENSION")
                text = retweetd['text']

            tweet['text'] = text

            ex = self.collection.find_one({'id': retweetd['id_str']})
            if ex:
                print("UPDATING")
                tweet['updated_at'] = time.time()
            else:
                tweet['inserted_at'] = time.time()

            self.collection.find_one_and_update(
                {'id': retweetd['id_str']},
                {'$set': tweet},
                upsert=True
            )

            print("Inserted tweet with id={}".format(status['id_str']))

    def on_error(self, status_code):
        if status_code == 420:
            print("Disconnected with 420 code")
            return False

        print(status_code)


if __name__ == '__main__':
    client = pymongo.MongoClient()
    db = client.local
    collection = db.tweets

    stream_listener = MyStreamListener(collection)
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)

    stream.filter(track=WORDS_LIST)
