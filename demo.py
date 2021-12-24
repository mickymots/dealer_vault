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


if __name__ == '__main__':
    token = get_token()
    print(token)