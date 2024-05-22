# This is a lightweight integration prototype that sends
# Satori audit data to PagerDuty. In this example we
# specify that any query which was blocked by Satori should also
# create a new incident in PagerDuty. 

# We also check for any access or self service rules. If we find any,
# then we should not trigger PagerDuty as this is a type of false positive.

# We further refine this integration by using the Satori Audit ID
# as our PagerDuty "incident ID" - this prevents duplicate incidents
# from showing up in PagerDuty. The mapping of a Satori Audit ID to
# a PagerDuty Incident ID results in nice, clean UPSERT behavior.

# Finally, we specified a 1 hour duration of audit data to pull from Satori.
# In business terms this prototype is meant to be automated in AWS Lambda,
# Google Cloud Run or similar, with a periodicity of 1 hour.

import json
import requests
import datetime
import time
import io

# REQUIRED
# Authenticate to Satori for a bearer token
# see https://app.satoricyber.com/docs/api for more info

satori_account_id = ""
satori_serviceaccount_id = ""
satori_serviceaccount_key = ""
satori_apihost = "app.satoricyber.com"

# REQUIRED
# PagerDuty API Key, this needs to be set up in their web UX

pagerduty_apikey = ""

# PagerDuty API URL, this can be left as-is for this example

pagerduty_incident_url = "https://api.pagerduty.com/incidents"

# REQUIRED
# the pager duty service ID, we tested using the "Default Service"
# you can get the ID by navigating to your PagerDuty Service and getting the ID from the URL

pagerduty_service_id = ""

# REQUIRED
# the pager duty FROM / SENT BY email address

pagerduty_sentby = "youremail@yourcompany.com"

# REQUIRED
# how many hours ago of Satori audit data do we want?

hours_ago = 1

# REQUIRED
# Which Satori action are we searching for?
# choices are: ACTION_NONE | ACTION_ALERT | ACTION_BLOCK | ACTION_MASK | ACTION_REQUEST_BLOCK | ACTION_ROW_FILTER

action_type = "ACTION_REQUEST_BLOCK" 


####################################################
# If you are just testing or learning you should 
# not have to change anything below this comment
####################################################


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


def get_custom_tag_audits():

    # Function to retrieve Satori audit entries from the last N hours of type 'action_type'
    
    unix_endtime = int(time.time()) * 1000
    unix_starttime = (int(time.time()) - (hours_ago * 3600)) * 1000
    
    # build request to rest API for audit entries, aka "data flows"
    # only search for queries which were blocked in the last 1 hour
    
    payload = {}
    headers = {'Authorization': 'Bearer {}'.format(satori_token),}
    auditurl = "https://{}/api/data-flow/{}/query?from={}&to={}&actionTypesFilter={}".format(
                                                                        satori_apihost,
                                                                        satori_account_id,
                                                                        unix_starttime,
                                                                        unix_endtime,
                                                                        action_type
                                                                         )
    try:
        response = requests.get(auditurl, headers=headers, data=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print("Retrieval of audit data failed: :", err)
        print("Exception TYPE:", type(err))
    else:
        return response

    
def get_access_rules(dataset_id):
    
    # function to get all access rules for the datasets
    
    # If any are found, then we should NOT send an alert to PagerDuty
    # because it's a false positive to assume that access was blocked
    # when in fact the end user could have the option to request access
    # using one of these access rules.
    
    rule_count = 0
    payload = {}
    headers = {'Authorization': 'Bearer {}'.format(satori_token),}
    
    # first type of access rule: manual approvals
    url = "https://{}/api/v1/data-access-rule/access-request?parentId={}".format(
                                                        satori_apihost,
                                                        dataset_id
                                                        )                                                        
    try:
        response = requests.get(url, headers=headers, data=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print("Retrieval of audit data failed: :", err)
        print("Exception TYPE:", type(err))
    else:
        rule_count += response.json()['count']

    # second type of access rule: self-service rules        
    url = "https://{}/api/v1/data-access-rule/self-service?parentId={}".format(
                                                        satori_apihost,
                                                        dataset_id
                                                        )                                                        
    try:
        response = requests.get(url, headers=headers, data=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print("Retrieval of audit data failed: :", err)
        print("Exception TYPE:", type(err))
    else:
        rule_count += response.json()['count']

    return rule_count

def send_to_pagerduty(flow_id, user, query):
    
    # Function to send Satori Audit Data to PagerDuty
    
    # We kept our PagerDuty POST payload to a bare minimum
    # You need to look up the Service ID from PagerDuty
    # For the PagerDuty incident_key we reuse the Satori Audit ID which is a great convenience!
    
    # From Satori, we pulled the audit id (flow_id), user email, and query that was run
    # There is a wealth of additional information you can send from Satori to PagerDuty.
    # see https://app.satoricyber.com/docs/api#get-/api/data-flow/-accountId-/query
    
    payload = {"incident": {
        "type": "incident",
        "title": "Satori Audit ID " + flow_id,
        "service": {
            "id": pagerduty_service_id,
            "type": "service_reference"
        },
        "urgency": "high",
        "incident_key": flow_id,
     
        "body": {
            "type": "incident_body",
            "details": user + " tried to run the following sensitive query but it was blocked by Satori:\n\n" + query
        }
    }}

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/vnd.pagerduty+json;version=2",
        "From": pagerduty_sentby,
        "Authorization": "Token token=" + pagerduty_apikey
    }

    try:
        response = requests.request("POST", pagerduty_incident_url, json=payload, headers=headers)
        print(response.json())
    except Exception as err:
        print("Exception TYPE:", type(err))


# MAIN CALLER
# This is just for testing purposes
# There are a thousand different ways to automate this :)
# for example in AWS Lambda you would wrap all of the above inside a lambda context function
# and then trigger the function using AWS CloudWatch and/or EventBridge.
# the next few lines are just for testing and study.
    
for item in get_custom_tag_audits().json()['records']:
    flow_id = str(item['flow_id'])
    user = str(item['identity']['name'])
    query = str(item['query'])
    datasets = item['datasets']
    total_rule_count = 0
    for dataset in datasets:
        # if you are testing and don't get a PagerDuty incident created
        # it means the Satori dataset associated with the audit ID has one or
        # more access rules and self-service rules defined
        if get_access_rules(dataset['dataset_id']) == 0:
            send_to_pagerduty(flow_id, user, query)