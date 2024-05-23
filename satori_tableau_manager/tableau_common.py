import requests
import json

requests.packages.urllib3.disable_warnings() 

def build_tableau_header(token):

	headers = {
		'X-Tableau-Auth': token,
		'Content-Type': 'application/json',
		'Accept': 'application/json'
		}
	return headers


def get_tableau_token(event_data):

	headers = {'Accept': 'application/json','Content-Type': 'application/json'}
	url = "https://" + event_data['tableau_base_url'] + "/api/" + event_data['tableau_api_version'] + "/auth/signin"
	payload = json.dumps({
			"credentials": {
				"personalAccessTokenName": event_data['tableau_pat_name'],
				"personalAccessTokenSecret": event_data['tableau_pat_secret'],
				"site": {
					"contentUrl": ""
					}
				}
			})
	response = requests.request("POST", url, headers=headers, data=payload, verify=event_data['verify_ssl'])
	site_id = response.json()['credentials']['site']['id']
	tableau_token = response.json()['credentials']['token']
	return tableau_token, site_id

def get_all_tableau_datasources(headers, event_data):

	tableau_pageSize = event_data['tableau_page_size']

	if (event_data['owner_url_encoded'] != '') and (event_data['owner'] != ''):
		tableau_filter = '&filter=ownerEmail:eq:' + event_data['owner_url_encoded']
	else:
		tableau_filter = ''

	# WE NEED TO PAGINATE OUR API CALLS IN CASE THERE ARE MORE THAN ONE THOUSAND DATASOURCES
	tableau_datasource_page_number = 1  # 1-based, not zero based
	tableau_datasource_total_returned = 0
	tableau_datasource_done = False
	tableau_datasources = []
	while not(tableau_datasource_done):
		paging_parameters = 'pageSize={}&pageNumber={page_number}'.format(
			event_data['tableau_page_size'], 
			page_number=tableau_datasource_page_number)
		url = event_data['tableau_url'] + "/datasources?" + paging_parameters + tableau_filter


		tableau_datasource_page = get_content(url, headers, event_data)
		for ds in tableau_datasource_page['datasources']['datasource']:
			tableau_datasources.append(ds)
		total_available = int(tableau_datasource_page['pagination']['totalAvailable'])
		tableau_datasource_page_number += 1
		tableau_datasource_total_returned += int(tableau_pageSize)
		if(tableau_datasource_total_returned >= total_available):
			tableau_datasource_done = True

	return tableau_datasources


def get_all_tableau_workbooks(headers, event_data):

	tableau_pageSize = event_data['tableau_page_size']

	if (event_data['owner_url_encoded'] != '') and (event_data['owner'] != ''):
		tableau_filter = '&filter=ownerEmail:eq:' + event_data['owner_url_encoded']
	else:
		tableau_filter = ''

	# WE NEED TO PAGINATE OUR API CALLS IN CASE THERE ARE MORE THAN ONE THOUSAND WORKBOOKS
	tableau_workbooks_page_number = 1  # 1-based, not zero based
	tableau_workbooks_total_returned = 0
	tableau_workbooks_done = False
	tableau_workbooks = []
	while not(tableau_workbooks_done):
		paging_parameters = 'pageSize={}&pageNumber={page_number}'.format(
			event_data['tableau_page_size'], 
			page_number=tableau_workbooks_page_number)
		url = event_data['tableau_url'] + "/workbooks?" + paging_parameters + tableau_filter
		tableau_workbooks_page = get_content(url, headers, event_data)
		for wb in tableau_workbooks_page['workbooks']['workbook']:
			tableau_workbooks.append(wb)
		total_available = int(tableau_workbooks_page['pagination']['totalAvailable'])
		tableau_workbooks_page_number += 1
		tableau_workbooks_total_returned += int(tableau_pageSize)
		if(tableau_workbooks_total_returned >= total_available):
			tableau_workbooks_done = True

	return tableau_workbooks

def get_content(url, header, event_data):

	payload = {}
	try:
		response = requests.request("GET", url, headers=header, data=payload, verify=event_data['verify_ssl'])
		return response.json()
	except requests.exceptions.RequestException as e:
		raise SystemExit(e)
		

def get_one_user(url, header, event_data):

	payload = {}
	try:
		response = requests.request("GET", url, headers=header, data=payload, verify=event_data['verify_ssl'])
		return response.json()
	except requests.exceptions.RequestException as e:
		raise SystemExit(e)


def update_one_connection(url, headers, newhostname, newusername, newpassword, event_data):

	print("\nupdating tableau connection: " + url)


	payload = json.dumps({
		"connection": {
		"serverAddress": newhostname,
		"userName": newusername,
		"password": newpassword,
		"embedPassword": "true",
		"queryTaggingEnabled": "true"
			}
		})

	print("\nupdating connection with this payload:\n" + payload)

	try:
		response = requests.request("PUT", url, headers=headers, data=payload, verify=event_data['verify_ssl'])
		return response.json()

	except requests.exceptions.RequestException as e:
		raise SystemExit(e)