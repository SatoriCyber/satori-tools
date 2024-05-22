# Simple Command line to find all Satori Datasets associated with a Satori Datastore.
# tested with python 3

# Usage:
#
# To find ONE Satori Datastore across all Satori Datasets,
# first get its ID from the UI and then at a prompt, run:
# python satori_find_datasets.py DATASTORE_ID
#
# To find ALL the Satori Datastores across all Satori Datasets,
# simply omit the datastore ID and run:
# python satori_find_datasets.py

# You must fill in the three Satori account variables below

import json
import requests
import sys

satori_account_id = "FILL_IN"
satori_serviceaccount_id = "FILL_IN"
satori_serviceaccount_key = "FILL_IN"
satori_apihost = "app.satoricyber.com"



###############################
## No changes below this line
## unless experimenting
###############################


def satori_auth(satori_serviceaccount_id, satori_serviceaccount_key, satori_apihost):
    
    # Get a Bearer Token for all the rest of this example
    
    auth_headers = {'content-type': 'application/json','accept': 'application/json'}
    auth_url = "https://{}/api/authentication/token".format(satori_apihost)
    auth_body = json.dumps(
    {
        "serviceAccountId": satori_serviceaccount_id,
        "serviceAccountKey": satori_serviceaccount_key
    })
    try:
        r = requests.post(auth_url, headers=auth_headers, data=auth_body)
        response = r.json()
        satori_token = response["token"]
    except Exception as err:
        print("Bearer Token Failure: :", err)
        print("Exception TYPE:", type(err))
    else:
        return satori_token


# our API bearer token for the rest of this example:
satori_token = satori_auth(satori_serviceaccount_id, satori_serviceaccount_key, satori_apihost)


def get_datasets():
    
    headers = {'Authorization': 'Bearer {}'.format(satori_token),}
    url = "https://{}/api/v1/dataset?accountId={}&pageSize=1000".format(
                                                        satori_apihost,
                                                        satori_account_id
                                                        )                                                        
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print("Retrieval of dataset data failed: :", err)
        print("Exception TYPE:", type(err))
    else:
        return response

def get_datastores():
    
    headers = {'Authorization': 'Bearer {}'.format(satori_token),}
    url = "https://{}/api/v1/datastore?accountId={}&pageSize=1000".format(
                                                        satori_apihost,
                                                        satori_account_id
                                                        )                                                        
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print("Retrieval of datastore data failed: :", err)
        print("Exception TYPE:", type(err))
    else:
        return response
    
def get_datastore_name(datastore_id):
    
    headers = {'Authorization': 'Bearer {}'.format(satori_token),}
    url = "https://{}/api/v1/datastore/{}".format(satori_apihost, datastore_id)                                                        
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print("Retrieval of name failed: :", err)
        print("Exception TYPE:", type(err))
    else:
        return response.json()['name']

def find_one_datastore(this_datastore_id):
    
    datastore_name = get_datastore_name(this_datastore_id)
    
    print('Finding all Datasets for Datastore "' + datastore_name + '"')

    datasets = get_datasets()
    
    print('\n{} is included in the following Datasets:'.format(datastore_name))
    
    for location in datasets.json()['records']:
        for item in location['includeLocations']:
            if item['dataStoreId']== this_datastore_id:
                print("  " + location['name'])
                
    print('\n{} is excluded in the following Datasets:'.format(datastore_name))
    
    for location in datasets.json()['records']:
        for item in location['excludeLocations']:
            if item['dataStoreId']== this_datastore_id:
                print("  " + location['name'])
                
def find_all_datastores():

    print('Finding all Datasets for all Datastores')

    datastores = get_datastores()
    datasets = get_datasets()
    
    for datastore in datastores.json()['records']:
        
        datastore_id = datastore['id']
        datastore_name = get_datastore_name(datastore_id)

        print('\nDatastore "{}" is included in these Datasets:'.format(datastore_name))

        for location in datasets.json()['records']:
            for item in location['includeLocations']:
                if item['dataStoreId']== datastore_id:
                    print("  " + location['name'])

        print('\nDatastore "{}" is excluded in these Datasets:'.format(datastore_name))

        for location in datasets.json()['records']:
            for item in location['excludeLocations']:
                if item['dataStoreId']== datastore_id:
                    print("  " + location['name'])
                    
        print("________________________________________________________________")


def main():

    if len(sys.argv) == 1:
        find_all_datastores()

    if len(sys.argv) == 2:
        find_one_datastore(sys.argv[1])

if __name__ == "__main__":
    main()