from dotenv import load_dotenv
import boto3
import json
import logging
from botocore.exceptions import ClientError
from base64 import b64decode
from urllib import request, error, parse
import requests
import os
from datetime import time, datetime, timedelta

logger = logging.getLogger()
logging.basicConfig()
logger.setLevel(logging.INFO)

load_dotenv()

QUEUE_URL = os.getenv('QUEUE_URL')
ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
SECRET_KEY = os.getenv('AWS_SECRET_KEY')
api_key = os.getenv('API_KEY')
api_url = os.getenv('API_URL')
vendor_id = os.getenv('VENDOR_PROGRAM_ID')
fileTypeCodes = os.getenv('FEED_TYPE_CODES')
bucket_name = os.getenv('bucket_name')

logger.info(f"Starting feed_poller.py")

s3 = boto3.resource('s3', region_name='ca-central-1')

feed_queue = boto3.client('sqs', region_name='ca-central-1')


def load_feed_history():
    feed_history = []
    obj = s3.Object(bucket_name, 'feed_date.json')
    logger.info(f"bucket_name = {bucket_name}")
    try:
        file_data = obj.get()['Body'].read()
        feed_history = json.loads(file_data)
        
        logger.debug(f"feed_history = {feed_history}")

    except ClientError as e:
        logger.error(f"ClientError = {e}")

    return feed_history


def get_token():

    api_key = os.getenv('API_KEY')
    api_url = os.getenv('API_URL')
    x_user_id = os.getenv('X_USER')

    headers = {'Cache-Control': 'no-cache',
               'Ocp-Apim-Subscription-Key': f'{api_key}', 'X-User': f'{x_user_id}'}

    # Get the JWT token
    response = requests.get(f'{api_url}/token', headers=headers)

    if response.status_code == 200:
        return response.json()['token']
    else:
        return None


def get_available_feeds(token, feed_history=None):

    headers = {'Cache-Control': 'no-cache',
               'Ocp-Apim-Subscription-Key': f'{api_key}', 'X-Jwt-Token': f'{token}'}

    feed_date = datetime.now()-timedelta(days=30)
    available_feeds = []
    if feed_history:
        
        for code in fileTypeCodes.split(','):
            
            feed = list(filter(lambda item: item['fileTypeCode'] == code, feed_history))

            if feed:
                feed_date = feed[0]['lastPollDate'].split('T')[0]

            response = requests.get(
                f'{api_url}/vendor/{vendor_id}/feeds/updated-data?fileTypeCode={code}&compareDate={feed_date}', headers=headers)
            available_feeds.extend(response.json())

    return available_feeds


def process_available_feeds(available_feeds):
    for feed in available_feeds:
        feed_queue.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(feed))
        logger.info(f"Feed = {feed}")


def lambda_handler(event, context):
    feed_history = load_feed_history()
    jwt_token = get_token()
    # print(jwt_token)

    logger.info(f"feed_history = {feed_history}")

    available_feeds = get_available_feeds(jwt_token, feed_history)
    process_available_feeds(available_feeds)
    return available_feeds
    

if __name__ == "__main__":
    lambda_handler(None, None)
