import json
import requests


# Example of copying Satori classifiers from Datastore A to Datastore B



#
# This scripts requires populated Inventory in Satori already.
# There are other scripts to automate inventory population in Satori
# Reminder: Inventory != Classifiers
#

#
# Account Info, needs to be updated
#

account_id = "SATORI_ACCOUNT_ID"
serviceaccount_id = "SATORI_SERVICE_ACCOUNT_ID"
serviceaccount_key = "SATORI_SERVICE_ACCOUNT_KEY"
apihost = "app.satoricyber.com"

#
# A source and target datastore ID, needs to be updated
#

source_datastore_id = "SATORI_DATASTORE_SOURCE"
target_datastore_id = "SATORI_DATASTORE_TARGET"

#
# Any Tables that you want to exclude, e.g. cruft or other non-Pii system tables, needs to be updated
#

exclude_tables = [
    "pg_attribute",
    "pg_class",
    "pg_constraint",
    "pg_extension",
    "pg_type",
    "pg_namespace",
    "pg_stat_ssl",
    "pg_catalog",
    "sysdac_history_internal",
    "sysdac_instances_internal",
    "database_firewall_rules",
    "ipv6_database_firewall_rules"    
]

#
# You should not have to change anything below this line
#


#
# Functions: auth, get source locations, update target locations
#

def satori_auth():
    headers = {'content-type': 'application/json', 'accept': 'application/json'}
    url = "https://{}/api/authentication/token".format(apihost)
    try:
        r = requests.post(url, 
                          headers=headers, 
                          data='{"serviceAccountId": "' + serviceaccount_id + 
                          '", "serviceAccountKey": "' + serviceaccount_key + '"}')
        response = r.json()
        satori_token = response["token"]
    except Exception as err:
        print("Bearer Token Failure: :", err)
        print("Exception TYPE:", type(err))
    else:
        return satori_token
    
def get_datastore_locations(satori_token, datastore_id):
    headers = {'Authorization': 'Bearer {}'.format(satori_token), 'Content-Type': 'application/json'}
    source_datastore_location_url = "https://{}/api/locations/{}/query?pageSize=1000&dataStoreId={}".format(apihost, account_id, datastore_id)

    try:
        source_datastore_location_response = requests.get(source_datastore_location_url, headers=headers)
        source_datastore_location_response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print("could not find data stores for this dataset: ", err)
        print("Exception TYPE:", type(err))
    else:
        return source_datastore_location_response
    
def update_datastore_locations(satori_token, location_id, tag_id):
    headers = {'Authorization': 'Bearer {}'.format(satori_token), 'Content-Type': 'application/json'}
    url = "https://app.satoricyber.com/api/locations/{}".format(location_id)

    try:
        update_tags_response = requests.put(url, headers=headers, data='{"addTags": ["' + tag_id + '"], "removeTags": [], "notes": "Updated via Satori Copy Inventory Script"}')
        update_tags_response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print("could not find data stores for this dataset: ", err)
        print("Exception TYPE:", type(err))
    else:
        #print(update_tags_response.text)
        return update_tags_response    


#
# MAIN WORK
#


#
# First, let's get all the locations for both our source and target datastores
#

token = satori_auth()
source_locations = get_datastore_locations(token, source_datastore_id)
target_locations = get_datastore_locations(token, target_datastore_id)


#
# Second, for any matches on Schema:Table:Column, update the TARGET DS with the tag from the SOURCE DS
#

for source_item in source_locations.json()['records']:
    schema = source_item['location']['schema']
    table = source_item['location']['table']
    column = source_item['location']['column']
    tags = source_item['tags']
    for target_item in target_locations.json()['records']:
        if table not in exclude_tables and \
        schema == target_item['location']['schema'] and \
        table == target_item['location']['table'] and \
        column == target_item['location']['column']:
            target_location_id = target_item['id']
            if tags:
                for tagitem in tags:
                    print("updating " + schema + ":" + table + ":" + column + " with tag: " + tagitem['name'])
                    update_datastore_locations(token, target_location_id, tagitem['name'])


# We can check afterward, our target datastore should have updated TAG IDs
#
# uncomment all the rest of this to check your results
#
#
#target_test_afterward = get_datastore_location(token, target_datastore_id)
#for items in target_test_afterward.json()['records']:
#
#    schema = items['location']['schema']
#    table = items['location']['table']
#    column = items['location']['column']
#    tags = items['tags']
#    if table not in exclude_tables:
#        print(schema + ":" + table + ":" + column + " has tag: ")
#        if tags:
#            for tagitem in tags:
#                print(tagitem['name'])
#        print(" ")