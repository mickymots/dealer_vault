from requests import api
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


ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
SECRET_KEY = os.getenv('AWS_SECRET_KEY')

s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY, region_name='ca-central-1')

sqs = boto3.client('sqs', aws_access_key_id=ACCESS_KEY,
                   aws_secret_access_key=SECRET_KEY, region_name='ca-central-1')


def get_token():
    return os.getenv('gatherup_api_key')


def get_agency_client():
    '''
    Get agency client list
    '''

    api_url = os.getenv('gatherup_api_url')
    api_version = os.getenv('gatherup_api_version')
    bearer_token = os.getenv('gatherup_bearer_token')

    headers = {'Cache-Control': 'no-cache', 'Content-Type': 'application/json',
               'Authorization': f'Bearer {bearer_token}'}

    response = requests.get(
        f'{api_url}/{api_version}/agency/clients?clientId={get_token()}', headers=headers)

    return response.json()


def get_client_business_list(agent_id):
    '''
    Get client business list - for example rainbow ford has 2 businesses
    '''

    api_url = os.getenv('gatherup_api_url')
    api_version = os.getenv('gatherup_api_version')
    bearer_token = os.getenv('gatherup_bearer_token')

    headers = {'Cache-Control': 'no-cache', 'Content-Type': 'application/json',
               'Authorization': f'Bearer {bearer_token}'}

    response = requests.get(
        f'{api_url}/{api_version}/businesses/get?clientId={get_token()}&agent={agent_id}', headers=headers)

    return response.json()


def get_business_id(vendor, file_type):
    '''
        Find the business id to create customer    
    '''

    # get vendor name
    logger.debug(f'vendor = {vendor}')
    logger.debug(f'file_type = {file_type}')

    agency_client = get_agency_client()

    business_info = list(filter(lambda x: x['name'].lower().find(
        vendor.lower()) > -1, agency_client['clients']))

    logger.info(f'business_info = {business_info}')

    # list businesses of the client
    business_list = get_client_business_list(business_info[0]['id'])

    logger.info(f'business_list = {business_list}')
    business_id = ''

    if len(business_list['data']) == 1:
        business_id = business_list['data'][0]['businessId']
    else:
        filtered_business = list(filter(lambda x: x['businessName'].lower().find(
            file_type.lower()) > -1, business_list['data']))
        if len(filtered_business) > 0:
            business_id = filtered_business[0]['businessId']
        else:
            raise Exception(
                f'Business not found for {vendor} and file type{file_type}')

    logger.debug(f'business_id = {business_id}')
    return business_id


def create_customer(customer, business_id):
    # pass
    clientId = get_token()
    api_url = os.getenv('gatherup_api_url')
    api_version = os.getenv('gatherup_api_version')
    bearer_token = os.getenv('gatherup_bearer_token')

    logger.info(f'business_id = {business_id}')
    business_id = 105215

    headers = {'Cache-Control': 'no-cache', 'Content-Type': 'application/json',
               'Authorization': f'Bearer {bearer_token}'}
    record = {"clientId": f"{clientId}",
              "agent": "447379",
              "businessId": f'{business_id}',
              "customerEmail": customer['Email'],
              "customerFirstName": customer['First Name'],
              "customerLastName": customer['Last Name'],

              "customerJobId": customer['Job ID'],
              "customerTags": customer['Tag'],

              }

    response = requests.post(f'{api_url}/{api_version}/customer/create', headers=headers,
                             json=record)

    logger.info(f"Response = {response.json()}")
    return customer, response.json()


def clean_records(json_data):

    dict_filter = lambda x, y: dict([ (i,x[i]) for i in x if i in set(y) ])

    records = []

    # Iterating through the json file
    for record in json_data:

        if record.get('Email'):
            logger.debug(f'Email = {record["Email"]}')
        elif record.get('Email 1'):
            logger.info(f"record = {record}")
            record['Email'] = record.pop('Email 1')
        elif record.get('Email 2'):
            record['Email'] = record.pop('Email 2')
        elif record.get('Email 3'):
            record['Email'] = record.pop('Email 3')
        
        else:
            record['Email'] = ''


        if record.get('Salesman 1 Name'):
            logger.debug(f"record = {record}")
            record['Tag'] = record.pop('Salesman 1 Name')
        elif record.get('Salesman Name'):
            logger.debug(f"record = {record}")
            record['Tag'] = record.pop('Salesman Name')
        elif record.get('Salesman'):
            logger.debug(f"record = {record}")
            record['Tag'] = record.pop('Salesman')
        elif record.get('Salesman 2 Name'):
            logger.debug(f"record = {record}")
            record['Tag'] = record.pop('Salesman 2 Name')
        elif record.get('Salesman Manager Name'):
            logger.debug(f"record = {record}")
            record['Tag'] = record.pop('Salesman Manager Name')
        else:
            record['Tag'] = ''


        record['Job ID'] =    record["Year"] + " " + record["Make"] + " " + record['Model']

        selected_dict_keys = ['Vendor Dealer ID', 'File Type', 'First Name', 'Last Name', 'Email', 'Year', 'Make', 'Model', 'Tag', 'Job ID']
    
        #lambda function to filter the dictionary
        

        parsed_data=dict_filter(record, selected_dict_keys)
        records.append(parsed_data)
        logger.debug(parsed_data)

    return records


def process_file(json_data):

    # logger.info(f"file_name = {file_name}")
    logger.info(f"processing file")

    # clean records
    parsed_data = clean_records(json_data)

    # select vendor name for identification
    business_id = get_business_id(
        parsed_data[0]['Vendor Dealer ID'], parsed_data[0]['File Type'])

    logger.info(f'business_id = {business_id}')

    # for item in parsed_data:
    #     # pass
    #     create_customer(item)

    create_request = list(
        map(lambda x:  create_customer(x, business_id), parsed_data))

    return create_request



# def process_file(file_name):

#     service_data = pd.read_json(f'/tmp/{file_name}', encoding='utf-8')

#     if 'Email 1' in service_data:
#         service_data.rename(columns={"Email 1": "Email"}, inplace=True)

#     if "Salesman 1 Name" in service_data:
#         service_data.rename(columns={"Salesman 1 Name": "Tag"}, inplace=True)

#     service_data["Job ID"] = service_data["Year"].astype(
#         str) + " " + service_data["Make"] + " " + service_data['Model']

#     selected_data = service_data[['First Name', 'Last Name', 'Email',
#                                   'Year', 'Make', 'Model', 'Tag', 'Job ID']]

#     parsed_data = json.loads(selected_data.to_json(orient='records'))

#     # select vendor name for identification
#     business_id = get_business_id(
#         service_data['Vendor Dealer ID'].values[0], service_data['File Type'].values[0])

#     logger.info(f'business_id = {business_id}')

#     # for item in parsed_data:
#     #     # pass
#     #     create_customer(item)

#     create_request = list(
#         map(lambda x:  create_customer(x, business_id), parsed_data))

#     return create_request

def archive_file(bucket_name, file_name):
    logger.info(f'file_name = {file_name}')
    s3.Object(bucket_name,f'archive/{file_name}.bkp').copy_from(CopySource=f'{bucket_name}/{file_name}')
    s3.Object(bucket_name,file_name).delete()


def download_file(s3_bucket, s3_key):
    '''
    This function will download the s3 file on tmp location.
    '''
    logger.info(f"bucket_name = {s3_bucket}")
    logger.info(f"file_name = {s3_key}")

    # bucket = s3.Bucket(s3_bucket)
    # bucket.download_file(s3_key, '/tmp/' + s3_key)


    content_object = s3.Object(s3_bucket, s3_key)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content


def lambda_handler(event, context):
    '''
    This function is the entry point for the Lambda function.

    '''
    logger.info("Lambda function started")
    load_dotenv()

    logger.setLevel(int(os.getenv('log_level')))
    token = get_token()
    try:

        for record in event['Records']:
            logger.info(f"record = {record}")

            s3_bucket = record['s3']['bucket']['name']
            s3_key = record['s3']['object']['key']

            logger.info(f"s3_bucket = {s3_bucket}")
            logger.info(f"s3_key = {s3_key}")

            json_data = download_file(s3_bucket, s3_key)
            response = process_file(json_data)
            logger.info(f"response = {response}")

            response = archive_file(s3_bucket, s3_key)
            logger.info(f"file archiveed")

    except Exception as e:
        logger.error(f"Error: {e}")
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('records created successfully')
    }


if __name__ == "__main__":
    logger.info('main')
    load_dotenv()

    event = json.loads(open('./s3_test.json').read())
    lambda_handler(event, None)
