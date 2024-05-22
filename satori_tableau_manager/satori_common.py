import requests
import json
import boto3
from botocore.exceptions import ClientError


def satori_auth(event_data):
	auth_headers = {'content-type': 'application/json','accept': 'application/json'}
	auth_url = "https://{}/api/authentication/token".format(event_data['satori_api_hostname'])
	auth_body = json.dumps(
	{
		"serviceAccountId": event_data['satori_sa_id'],
		"serviceAccountKey": event_data['satori_sa_key']
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

def satori_build_header(satori_token):
	auth_headers = {
	'Authorization': 'Bearer {}'.format(satori_token), 
	'Content-Type': 'application/json', 
	'Accept': 'application/json'
	}
	return auth_headers

def get_all_users(headers, event_data):

	url =  "https://{}/api/v1/users?accountId={}&pageSize=5000".format(
		event_data['satori_api_hostname'], 
		event_data['satori_account_id'])

	print("getting all satori users: " + url)

	try:
		response = requests.get(url, headers=headers)
		response.raise_for_status()
	except requests.exceptions.RequestException as err:
		print("EXCEPTION: ", type(err))
	else:
		return response.json()['records']

def get_one_user(headers, satori_account_id, apihost, email):


	email = email.replace('+','%2B')

	url =  "https://{}/api/v1/users/profile?accountId={}&email={}".format(
		apihost, 
		satori_account_id,
		email)
	print("\n\ngetting satori user: " + url)

	try:
		response = requests.get(url, headers=headers)
		response.raise_for_status()
	except requests.exceptions.RequestException as err:
		print("get one user ERROR OCCURRED: " + str(response.status_code))
		print("EXCEPTION: ", type(err))
	else:
		return response.json()


def get_all_datastores(headers, event_data):

	url =  "https://{}/api/v1/datastore?accountId={}&pageSize=5000".format(
		event_data['satori_api_hostname'], 
		event_data['satori_account_id'])
	print("getting all satori datastores: " + url)

	try:
		response = requests.get(url, headers=headers)
		response.raise_for_status()
	except requests.exceptions.RequestException as err:
		print("EXCEPTION: ", type(err))
	else:
		return response.json()['records']


def get_one_datastore(headers, apihost, datastore_id):

	url =  "https://{}/api/v1/datastore/{}".format(apihost, datastore_id)
	print("\n\ngetting one satori datastore: " + url)

	try:
		response = requests.get(url, headers=headers)
		response.raise_for_status()
	except requests.exceptions.RequestException as err:
		print("get one datastore ERROR OCCURRED: " + str(response.status_code))
		print("EXCEPTION: ", type(err))
	else:
		return response.json()


def generate_satori_pat(headers, apihost, user_id, satori_pat_name):

	url = "https://{}/api/users/{}/personal-access-tokens".format(apihost, user_id)

	payload = json.dumps({
		"tokenName": satori_pat_name,
		"expirationPeriod": 180
			})

	try:
		response = requests.post(url, headers=headers, data=payload)
		response.raise_for_status()
	except requests.exceptions.RequestException as err:
		print("generate Satori PAT ERROR OCCURRED")
		print(response.text)
		print("EXCEPTION: ", type(err))
		return response.json(), response.status_code
	else:
		return response.json(), response.status_code


