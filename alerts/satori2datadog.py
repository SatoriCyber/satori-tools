import json
import requests
import datetime
import io

#REQUIRED
# Authenticate to Satori for a bearer token
satori_account_id = 'NEED_VALUE'
satori_serviceaccount_id = 'NEED_VALUE'
satori_serviceaccount_key = 'NEED_VALUE'

#REQUIRED
# we will use the datadog 'event' api
# Authenticate to Datadog using app key and api key
datadog_headers = {
  'DD-APPLICATION-KEY': 'NEED_VALUE',
  'DD-API-KEY': 'NEED_VALUE',
  'Content-Type': 'application/json'
}

#REQUIRED
#how many days ago of Satori audit data do we want?
days_ago = 1

#which custom Satori tag are we searching for?
custom_tagname = "custom_classifier"

#default endpoints for both Satori and Datadog. Only change these if instructed to
datadog_url = "https://api.datadoghq.com/api/v1/events"
satori_apihost = "app.satoricyber.com"

## for a simple demo, no changes below this line

def get_custom_tag_audits():

    # This function retrieves Satori audit entries from the last NN days up to yesterday
    yesterday_start = datetime.date.today() - datetime.timedelta(days_ago)
    unix_time_start = yesterday_start.strftime("%s") + "000"
    unix_time_end = str(int(yesterday_start.strftime("%s")) + (86400 * days_ago + 86400)) + "000"
    
    # Authenticate to Satori for a bearer token
    authheaders = {'content-type': 'application/json','accept': 'application/json'}
    url = "https://{}/api/authentication/token".format(satori_apihost)
    try:
        r = requests.post(url, 
                          headers=authheaders, 
                          data='{"serviceAccountId": "' + satori_serviceaccount_id + 
                          '", "serviceAccountKey": "' + satori_serviceaccount_key + '"}')
        response = r.json()
        satori_token = response["token"]
    except Exception as err:
        print("Bearer Token Failure: :", err)
        print("Exception TYPE:", type(err))


    # build request to rest API for audit entries, aka "data flows"
    payload = {}
    headers = {'Authorization': 'Bearer {}'.format(satori_token),}
    auditurl = "https://{}/api/data-flow/{}/query?from={}&to={}&tagsFilter={}".format(satori_apihost,
                                                                         satori_account_id,
                                                                         unix_time_start,
                                                                         unix_time_end,
                                                                         custom_tagname)
    try:
        response = requests.get(auditurl, headers=headers, data=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print("Retrieval of audit data failed: :", err)
        print("Exception TYPE:", type(err))
    else:
        return response
        

def lambda_handler(event, context):

    datadog_payload = get_custom_tag_audits()
    #for each record retrieved, add to Datadog as an event
    #for each record retrieved, add to Datadog as an event
    for item in datadog_payload.json()['records']:
    
        # collate a URL back to Satori
        
        satori_callback_url = "https://app.satoricyber.com/audit?timeFrame=last90days&flowId=" + item['flow_id']
       
        # generate an WARNING alert if under 500 records, else an ERROR if over
        alert_type = "warning" if item['records']['value'] < 500 else "error"
        
        # let's collate all of the tags that were found by Satori for this query
        tags = ''
        for tagname in item['tags']:
            tags += tagname['name'] + "\n"
        datadog_payload = json.dumps({
            "alert_type": alert_type,
            "source_type_name" : "amazon lambda",
            
            #change your Datadog event title as desired
            "title": "WARNING! SENSITIVE QUERY RUN BY: " + item['identity']['name'] + " at " + item['flow_timestamp'], 
            
            #'original_query', 'tags' are from the original json payload from Satori
            "text": "%%% *" + item['query']['original_query'] + "* \n\n[Open Audit Data in Satori](" + satori_callback_url + ")\n\nUser Identity: " + item['identity']['name'] + "\nRecords Returned: " + str(item['records']['value']) + "\n\nSatori Inventory Tags:\n" + tags + "\n %%%", 
            
            #The next line is a Datadog tag. Change or add Datadog tags here as desired
            "tags": [
            "SATORI_AUDIT_DATA" 
          ]
        })
        try:
            response = requests.request("POST", datadog_url, headers=datadog_headers, data=datadog_payload)
            print(response.json()['event']['url'])
        except requests.exceptions.RequestException as err:
            print("sending of audit data failed: :", err)
            print("Exception TYPE:", type(err))