import requests
import os
from dotenv import load_dotenv
import time


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

def get_delivery_status(token, requestId):
    api_key = os.getenv('API_KEY')
    api_url = os.getenv('API_URL')
  
    headers = {'Cache-Control': 'no-cache','Ocp-Apim-Subscription-Key': f'{api_key}', 'X-Jwt-Token': f'{token}'}
    response = requests.get(f'{api_url}/delivery/{requestId}', headers=headers)

    print(f"delivery status = {response.json()['status']}")

    return response.json()


def initiate_delivery(token):
    api_key = os.getenv('API_KEY')
    api_url = os.getenv('API_URL')
    vendor_id = os.getenv('VENDOR_PROGRAM_ID')
    

    headers = {'Cache-Control': 'no-cache','Ocp-Apim-Subscription-Key': f'{api_key}', 'X-Jwt-Token': f'{token}'}
    response = requests.post(f'{api_url}/delivery', headers=headers, 
                json={"programId": f"{vendor_id}", "rooftopId": "DVD52222", "fileType": "SV", 
                    "options": {        "type": "Catchup",
                        # "historicalMonths": 0,
                        "catchupStartDate": "2021-10-01",
                        "catchupEndDate": "2021-10-30"
                    }
                })

    print(f"requestId = {response.json()['requestId']}")

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




def main():
    load_dotenv()
    jwt_token = get_token()
    
    print(jwt_token)

    if  jwt_token:
        
        feed = initiate_delivery(jwt_token)
        status = feed['status']

        #Queued, InProgress, Ready, Error, Purged.
        while status != 'Ready':
            print(f"status = {status}")
            feed = get_delivery_status(jwt_token, feed['requestId'])
            status = feed['status']
            print(f"status = {status}")
            if status == 'Error':
                print(f"status = {status}")
                break
            else:
                time.sleep(15) # wait 15 seconds before checking again
                continue

        # GET the dataset
        dataset = get_dataset(jwt_token, feed['requestId'])       

        # print(f'dataset length = {len(dataset)}')
    else:
        print('Token is invalid')

   

if __name__ == "__main__":
    main()