import urllib3
import boto3
import json
import logging
import os
import ast
from botocore.exceptions import ClientError
from base64 import b64decode
from urllib import request, error, parse
import requests
import os
from dotenv import load_dotenv
from datetime import time, datetime, timedelta

logger = logging.getLogger()
logging.basicConfig()
logger.setLevel(logging.INFO)


ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
SECRET_KEY = os.getenv('AWS_SECRET_KEY')

s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY, region_name='ca-central-1')

sqs = boto3.client('sqs',aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY, region_name='ca-central-1')




def load_feed_date():
    feed_history = []
    bucket_name = os.getenv('BUCKET_NAME')
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
   


def initiate_delivery(payload, catchup_start_date):
    '''
    This function initiates the delivery of the feed.
    '''
    
    logger.info('initiate delivery')

    token = get_token()
    if token is None:
        logger.error('Token is None')
        return

    api_key = os.getenv('API_KEY')
    api_url = os.getenv('API_URL')
    vendor_id = os.getenv('VENDOR_PROGRAM_ID')

    payload = json.loads(payload)
    roof_top_id = payload['dvdId']
    file_type_code = payload['fileTypeCode']
    catchup_end_date = payload['lastPollDate'].split('T')[0]
    headers = {'Cache-Control': 'no-cache',
               'Ocp-Apim-Subscription-Key': f'{api_key}', 'X-Jwt-Token': f'{token}'}
    response = requests.post(f'{api_url}/delivery', headers=headers,
                             json={"programId": f"{vendor_id}", "rooftopId": f"{roof_top_id}", "fileType": f"{file_type_code}",
                                   "options": {"type": "Catchup",
                                               # "historicalMonths": 0,
                                               "catchupStartDate": f"{catchup_start_date}",
                                               "catchupEndDate": f"{catchup_end_date}",
                                               }
                                   })

    logger.info(f"requestId = {response.json()}")

    return response.json()


def list_filter(dealer):
    '''
        Creates a filter function to filter out the history of the feed for the given dealer
        This helps to avoid duplicate delivery requests
    '''

    def filter_dealer(feed_history):
        result =  feed_history['dvdId'] == dealer['dvdId'] and feed_history['fileTypeCode'] == dealer['fileTypeCode'] 
        logger.debug(f"result = {result}")
        return result

    return filter_dealer


def get_catchup_start_date():
    '''
        This function returns the start date for the catchup delivery if the last poll date is not available
    '''
    catchup_start_date = datetime.now()-timedelta(days=30)
    catchup_start_date = catchup_start_date.strftime('%Y-%m-%d')
   
    return catchup_start_date



def lambda_handler(event, context):
    '''
    This function is the entry point for the Lambda function.

    '''
    logger.info("Lambda function started")

    logger.info(f"event = {event}")
    
    feed_history = load_feed_date()

    for record in event['Records']:
        logger.info(f"Record: {record}")

        payload = record["body"]
       
        listfilter = list_filter(json.loads(payload))
        filterd_history = list(filter(listfilter, feed_history))
        
        catchup_start_date = get_catchup_start_date()
        logger.info(f"catchup_start_date = {catchup_start_date}")

        if len(filterd_history) != 0:
            catchup_start_date = filterd_history[0]['lastPollDate'].split('T')[0]
        
        logger.info(f"catchup_start_date = {catchup_start_date}")
        response = initiate_delivery(payload, catchup_start_date)
        sqs.send_message(QueueUrl=os.getenv('REQUEST_QUEUE_URL'), MessageBody=json.dumps(response))


if __name__ == "__main__":
    logger.info('main')
    load_dotenv()
    event = {'Records': [
        {'body': '{"lastPollDate": "2021-12-11T23:24:40", "dealerClientId": "DVD43550", "dvdId": "DVD43550", "fileTypeCode": "SV"}'}]}

    lambda_handler(event, None)
