import requests
import json

requests.packages.urllib3.disable_warnings() 

def satori_auth(event_data):
	auth_headers = {'content-type': 'application/json','accept': 'application/json'}
	auth_url = "https://{}/api/authentication/token".format(event_data['satori_api_hostname'])
	auth_body = json.dumps(
	{
		"serviceAccountId": event_data['satori_sa_id'],
		"serviceAccountKey": event_data['satori_sa_key']
	})
	try:
		r = requests.post(auth_url, headers=auth_headers, data=auth_body, verify=event_data['verify_ssl'])
		response = r.json()
		satori_token = response["token"]
	except Exception as err:
		print("Bearer Token Failure: :", err)
		print("Exception TYPE:", type(err))
	else:
		return satori_token

def satori_build_header(satori_token):
	auth_headers = {
	'Authorization': 'Bearer {}'.format(satori_token), 
	'Content-Type': 'application/json', 
	'Accept': 'application/json'
	}
	return auth_headers

def get_all_users(headers, event_data):

	#will only retrieve 5000 Satori users

	url =  "https://{}/api/v1/users?accountId={}&pageSize=5000".format(
		event_data['satori_api_hostname'], 
		event_data['satori_account_id'])

	print("getting all satori users: " + url)

	try:
		response = requests.get(url, headers=headers, verify=event_data['verify_ssl'])
		response.raise_for_status()
	except requests.exceptions.RequestException as err:
		print("EXCEPTION: ", type(err))
	else:
		return response.json()['records']

def get_one_user(headers, event_data):

	email = event_data['content_owner'].replace('+','%2B') 
	
	url =  "https://{}/api/v1/users/profile?accountId={}&email={}".format(
		event_data['satori_api_hostname'], 
		event_data['satori_account_id'],
		email)
	print("\n\ngetting satori user: " + url)

	try:
		response = requests.get(url, headers=headers, verify=event_data['verify_ssl'])
		response.raise_for_status()
	except requests.exceptions.RequestException as err:
		print("get one user ERROR OCCURRED")
		print(response.status_code)
		print("EXCEPTION: ", type(err))
	else:
		return response.json()


def get_all_datastores(headers, event_data):

	#will only return 5000 Satori Datastores

	url =  "https://{}/api/v1/datastore?accountId={}&pageSize=5000".format(
		event_data['satori_api_hostname'], 
		event_data['satori_account_id'])
	print("getting all satori datastores: " + url)

	try:
		response = requests.get(url, headers=headers, verify=event_data['verify_ssl'])
		response.raise_for_status()
	except requests.exceptions.RequestException as err:
		print("EXCEPTION: ", type(err))
	else:
		return response.json()['records']


def get_one_datastore(headers, event_data):

	datastore_url =  "https://{}/api/v1/datastore/{}".format(
		event_data['satori_api_hostname'], 
		event_data['datastore_id'])
	print("\n\ngetting one satori datastore: " + datastore_url)

	try:
		response = requests.get(datastore_url, headers=headers, verify=False)
		response.raise_for_status()
	except requests.exceptions.RequestException as err:
		print("EXCEPTION: ", type(err))
	else:
		return response.json()


def generate_satori_pat(headers, event_data):

	#New Satori API as of Spring 2024: create a PAT for a Satori User (by ID)

	url = "https://{}/api/users/{}/personal-access-tokens".format(
		event_data['satori_api_hostname'], 
		event_data['satori_user_id'])

	payload = json.dumps({
		"tokenName": event_data['satori_newpatname'],
		"expirationPeriod": 180
			})

	try:
		response = requests.post(url, headers=headers, data=payload, verify=event_data['verify_ssl'])
		response.raise_for_status()
	except requests.exceptions.RequestException as err:
		print("generate Satori PAT ERROR OCCURRED")
		print(response.text)
		print("EXCEPTION: ", type(err))
		return response.json(), response.status_code
	else:
		return response.json(), response.status_code


