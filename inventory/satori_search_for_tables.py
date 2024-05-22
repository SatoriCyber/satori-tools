# USAGE
# Given a full location path in a Satori Dataset like "db.schema.tablename"
# this utility lets you search across all Datasets for any partial substring
# e.g. using the above example, searching for "hema.tabl" would match "db.schema.tablename"

# At a command line: python searchtable.py <searchstring>

# Save this file, and then fill in the account id, service id and key values below

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

def construct_full_path(location):
    full_path = ''
    if location['location'] is not None:
        full_path += str(location['location']['db']) + "." if 'db' in location['location'] and location['location']['db'] is not None else '*.'
        full_path += str(location['location']['schema']) + "." if 'schema' in location['location'] and location['location']['schema'] is not None else '*.'
        full_path += str(location['location']['table']) if 'table' in location['location']  and location['location']['table'] is not None else '*'
    return full_path

def search(search_location):

    datasets = get_datasets().json()['records']
    datastores = get_datastores().json()['records']    
    results = []
    datastore_info = {}
    for datastore in datastores:
        datastore_info[datastore['id']] = datastore['name']
    
    print('searching for location: ' + str(search_location) + '\n')
    for dataset in datasets:
    
        for location in dataset['includeLocations']:
            full_path = construct_full_path(location)
            if search_location in full_path:
                datastore_name = datastore_info[location['dataStoreId']]
                results.append(
                               'Included Location: ' + full_path + '\n' +
                               'Dataset: ' + dataset['name'] + ' (' + dataset['id'] + ')\n' +
                               'https://app.satoricyber.com/datasets/' + dataset['id'] + '/definition' + '\n'
                               'Datastore: ' + datastore_name + '(' + location['dataStoreId'] + ')\n'
                               'https://app.satoricyber.com/data-stores/' + location['dataStoreId'] + '/settings' + '\n'
                              )
        for location in dataset['excludeLocations']:
            full_path = construct_full_path(location)
            if search_location in full_path:
                datastore_name = datastore_info[location['dataStoreId']]
                results.append(
                               'Excluded Location: ' + full_path + '\n' +
                               'Dataset: ' + dataset['name'] + ' (' + dataset['id'] + ')\n' +
                               'https://app.satoricyber.com/datasets/' + dataset['id'] + '/definition' + '\n'
                               'Datastore: ' + datastore_name + '(' + location['dataStoreId'] + ')\n'
                               'https://app.satoricyber.com/data-stores/' + location['dataStoreId'] + '/settings' + '\n'
                              )
    
    #print all collated results
    print('results found: ' + str(len(results)) + "\n")
    for x in range(len(results)):
        print(x+1)
        print(results[x])
    print('results found: ' + str(len(results)) + "\n")
                    

satori_token = satori_auth(satori_serviceaccount_id, satori_serviceaccount_key, satori_apihost)

def main():
    if len(sys.argv) == 2:
        search(str(sys.argv[1]))

if __name__ == "__main__":
    main()