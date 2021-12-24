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
import time as t

logger = logging.getLogger()
logging.basicConfig()
logger.setLevel(logging.INFO)


ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
SECRET_KEY = os.getenv('AWS_SECRET_KEY')

s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY, region_name='ca-central-1')

sqs = boto3.client('sqs',aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY, region_name='ca-central-1')


def get_token():

    api_key = os.getenv('API_KEY')
    api_url = os.getenv('API_URL')
    x_user_id = os.getenv('X_USER')

    headers = {'Cache-Control': 'no-cache',
               'Ocp-Apim-Subscription-Key': f'{api_key}', 'X-User': f'{x_user_id}'}

    
    response = requests.get(f'{api_url}/token', headers=headers)
    logger.info(f"get_token response = {response}")
    if response.status_code == 200:
        return response.json()['token']
    else:
        return None

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

def get_delivery_status(token, requestId):
    logger.info(f"get_delivery_status for token = {token}, requestId = {requestId}")
    api_key = os.getenv('API_KEY')
    api_url = os.getenv('API_URL')
  
    headers = {'Cache-Control': 'no-cache','Ocp-Apim-Subscription-Key': f'{api_key}', 'X-Jwt-Token': f'{token}'}
    response = requests.get(f'{api_url}/delivery/{requestId}', headers=headers)
    logger.info(f"get_delivery_status response = {response}")
    return response.json()

def get_dataset(token, requestId):
    api_key = os.getenv('API_KEY')
    api_url = os.getenv('API_URL')
    pageSize = os.getenv('PAGE_SIZE')

    headers = {'Cache-Control': 'no-cache','Ocp-Apim-Subscription-Key': f'{api_key}', 'X-Jwt-Token': f'{token}'}
    response = requests.get(f'{api_url}/delivery?requestId={requestId}&pageSize={pageSize}', headers=headers)

    total_records = response.json()['totalRecords']
    totalRecordsInPage = response.json()['totalRecordsInPage']
    
    total_records = response.json()['totalRecords']
    print(f" total_records = {total_records}, totalRecordsInPage = {totalRecordsInPage}")


    continuationToken = response.json().get('continuationToken')
    records = response.json()['records']
    while continuationToken:
        
        response = requests.get(f'{api_url}/delivery?requestId={requestId}&pageSize={pageSize}&continuationToken={continuationToken}', headers=headers)
        totalRecordsInPage = response.json()['totalRecordsInPage']
        continuationToken = response.json().get('continuationToken')
        print(f"totalRecordsInPage = {totalRecordsInPage}")
        records.extend(response.json()['records'])
       
    print(f"total dataset = {len(records)}")

    return records


def process_dataset(request_id,dataset):
    logger.info(f"process_dataset for request_id = {request_id}")
    bucket_name = os.getenv('BUCKET_NAME')
    s3object = s3.Object(bucket_name, f'{request_id}.json')

    s3object.put(
        Body=(bytes(json.dumps(dataset).encode('UTF-8')))
    )


def update_feed_tracker(delivery_status):
    bucket_name = os.getenv('BUCKET_NAME')

    rooftopId = delivery_status['initialRequest']['rooftopId']
    fileTypeCode = delivery_status['initialRequest']['fileType']
    lastPollDate = delivery_status['initialRequest']['options']['catchupEndDate']
    logger.info(f"rooftopId = {rooftopId}, fileTypeCode = {fileTypeCode}, lastPollDate = {lastPollDate}")

    obj = s3.Object(bucket_name, 'feed_date.json')
    file_data = obj.get()['Body'].read()
    feed_history = json.loads(file_data)

    is_history_updated = False

    for feed in feed_history:
        if feed['dvdId'] == rooftopId and feed['fileTypeCode'] == fileTypeCode:
            feed['lastPollDate'] = lastPollDate
            is_history_updated = True
            break
    
    if not is_history_updated:
        feed_history.append({'dvdId': rooftopId, 'fileTypeCode': fileTypeCode, 'lastPollDate': lastPollDate})
    
    logger.info(f"feed_history = {feed_history}")
    obj.put(
        Body=(bytes(json.dumps(feed_history).encode('UTF-8')))
    )

    return feed_history      
       
    
    

def lambda_handler(event, context):
    '''
    This function is the entry point for the Lambda function.

    '''
    logger.info("Lambda function started")
    load_dotenv()
    token = get_token()
    delivery_status = None
    for record in event['Records']:
        
        logger.info(f"Record: {record}")

        payload = json.loads(record["body"])
        status = 'Queued'
        #Queued, InProgress, Ready, Error, Purged.
        retryCount = 0
        while status != 'Ready':
           
            delivery_status = get_delivery_status(token, payload['requestId'])
            
            logger.info(f"delivery_status = {delivery_status}")
            status = delivery_status['status']
            logger.info(f"status = {status}")
            if status == 'Error' and retryCount < 3:
                print(f"status = {status}")
                break
            else:
                retryCount += 1
                t.sleep(20) # wait 20 seconds before checking again 3 times
                continue

        # GET the dataset
        if status == 'Ready':
            dataset = get_dataset(token, delivery_status['requestId'])       
            process_dataset(payload['requestId'], dataset)
            feed_trakcer = update_feed_tracker(delivery_status)
            return feed_trakcer
       
        else:
            logger.error('feed processing failed')
            return json.dumps({'StatusCode': 400, 'status': 'feed processing failed'})


if __name__ == "__main__":
    logger.info('main')
    load_dotenv()
    event = {'Records': [
        {'body': '{"requestId": "bae0083c-e9c5-4e80-9d5f-6bf985add7e6", "status": "Queued", "initialRequest": {"programId": "DVV01355", "rooftopId": "DVD43550", "fileType": "SV", "options": {"type": "Catchup", "catchupStartDate": "2021-10-01T00:00:00", "catchupEndDate": "2021-12-11T00:00:00"}}, "createdDate": "2021-12-18T20:56:32.9216236+00:00", "createdBy": "amir@d20g.com", "recordCount": 0}'}]}

    lambda_handler(event, None)
