# Simple Command line to group all downstream Satori Datastores and Datasets by each Satori DAC
# tested with python 3

# Usage:
# at a command line run:
# python satori_get_all_dac_info.py

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

def get_dacs():
    
    headers = {'Authorization': 'Bearer {}'.format(satori_token),}
    url = "https://{}/api/v1/data-access-controllers?accountId={}&pageSize=1000".format(
                                                        satori_apihost,
                                                        satori_account_id
                                                        )                                                        
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print("Retrieval of dac data failed: :", err)
        print("Exception TYPE:", type(err))
    else:
        return response

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
        print("Retrieval of audit data failed: :", err)
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
        print("Retrieval of audit data failed: :", err)
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
        print("Retrieval of audit data failed: :", err)
        print("Exception TYPE:", type(err))
    else:
        return response.json()['name']

def find_one_datastore(this_datastore_id):
    
    datastore_name = get_datastore_name(this_datastore_id)
    
    datasets = get_datasets()
    
    print('         included in the following Datasets'.format(datastore_name))
    
    for location in datasets.json()['records']:
        for item in location['includeLocations']:
            if item['dataStoreId']== this_datastore_id:
                print("              " + location['name'])
                
    print('         excluded in the following Datasets'.format(datastore_name))
    
    for location in datasets.json()['records']:
        for item in location['excludeLocations']:
            if item['dataStoreId']== this_datastore_id:
                print("              " + location['name'])

def group_everything_by_dac():

    datasets = get_datasets()
    datastores = get_datastores()
        
    for dac in get_dacs().json()['records']:
        print('DAC NAME: ' + dac['name'] + "\n")
        for datastore in datastores.json()['records']:
            if datastore['dataAccessControllerId'] == dac['id']:
                print('\n   Datastore Name: ' + datastore['name'] + '\n   Datastore ID: ' + datastore['id'])
                find_one_datastore(datastore['id'])
        print("\n")
        print("__________________________________________________________\n")


def main():

    group_everything_by_dac()

if __name__ == "__main__":
    main()