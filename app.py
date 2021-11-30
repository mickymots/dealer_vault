import requests
import os
from dotenv import load_dotenv


def get_token():
    
    api_key = os.getenv('API_KEY')
    api_url = os.getenv('API_URL')
    x_user_id = os.getenv('X_USER')

    headers = {'Cache-Control': 'no-cache','Ocp-Apim-Subscription-Key': f'{api_key}', 'X-User': f'{x_user_id}'}

    # Get the JWT token
    response = requests.get(f'{api_url}/token', headers=headers)
    
    if response.status_code == 200:
        return response.json()['token']
    else:
        return None

def get_feed(token):
    api_key = os.getenv('API_KEY')
    api_url = os.getenv('API_URL')
    feed_date= os.getenv('FEED_DATE')
    vendor_id = os.getenv('VENDOR_PROGRAM_ID')
    fileTypeCode = 'SV'

    headers = {'Cache-Control': 'no-cache','Ocp-Apim-Subscription-Key': f'{api_key}', 'X-Jwt-Token': f'{token}'}
    response = requests.get(f'{api_url}/vendor/{vendor_id}/feeds/updated-data?fileTypeCode={fileTypeCode}&compareDate={feed_date}', headers=headers)

    return response.json()


def initiate_feed(token):
    api_key = os.getenv('API_KEY')
    api_url = os.getenv('API_URL')
    vendor_id = os.getenv('VENDOR_PROGRAM_ID')
    fileTypeCode = 'SV'

    headers = {'Cache-Control': 'no-cache','Ocp-Apim-Subscription-Key': f'{api_key}', 'X-Jwt-Token': f'{token}'}
    response = requests.post(f'{api_url}/vendor/{vendor_id}/feeds/initiate?fileTypeCode={fileTypeCode}', headers=headers)

    return response.json()
    

def main():
    load_dotenv()
    jwt_token = get_token()
    
    print(jwt_token)

    if  jwt_token:
        print('Token is valid')
        feed = get_feed(jwt_token)
        print(feed)
    else:
        print('Token is invalid')

   

if __name__ == "__main__":
    main()