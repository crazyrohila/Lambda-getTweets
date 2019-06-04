import os
import sys
# where libraries are installed
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/lib")

import boto3
import requests
import json
import time

s3 = boto3.resource('s3')
bucket = s3.Bucket('india-history-pics')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('india-history-pics')

url = "https://api.twitter.com/1.1/tweets/search/fullarchive/dev.json"

payload = {
    "query": "from:crazyrohila",
    "fromDate": "201401010000",
    "toDate": "201801010000", #format - yyyymmddhhmm
    "maxResults": 100
}

headers = {
    'Authorization': "Bearer <bearer-token>",
    'Content-Type': "application/json",
}

counter = 1

def lambda_handler(event, context):
    payload = event if ('query' in event) else payload
    getTweets(payload)

def getTweets(payload):
    global counter
    counter += 1
    response = requests.request("POST", url, data=json.dumps(payload), headers=headers)
    print('response', response)
    res = json.loads(response.text)
    # print('response body', res)
    if ('results' in res):
        processTweets(res['results'])
    if ('next' in res and res['next']):
        payload['next'] = res['next']
        getTweets(payload)
    else:
        print(counter)

def processTweets(results):
    print('rr',len(results))
    tweets = []
    for tweet in results:
        if ('entities' in tweet and 'media' in tweet['entities'] and tweet['entities']['media']):
            text = tweet['extended_tweet']['full_text'] if ('extended_tweet' in tweet) else tweet['text']
            createdAt = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
            t = {
                "id": tweet['id'],
                "created_at": createdAt,
                "text": text,
                "images": []
            }
            if ('::' in text):
                yearstr = text.split('::')
                if (yearstr):
                    t['year'] = yearstr[0]

            for image in tweet['entities']['media']:
                if (image['type'] == 'photo'):
                    t['images'].append({
                        "id": image['id'],
                        "url": image['media_url_https']
                    })
                    saveImage(tweet['id'], image['media_url_https'])
            tweets.append(t)
    saveToDB(tweets)

def saveToDB(tweets):
    print('tweets', len(tweets))
    with table.batch_writer(overwrite_by_pkeys=['id']) as batch:
        for tweet in tweets:
            batch.put_item(
                Item=tweet
            )

def saveImage(tweet_id, image_url):
    key = str(tweet_id) + '-' + image_url.split('/')[-1]
    r = requests.get(image_url, stream=True)
    bucket.upload_fileobj(r.raw, key)
