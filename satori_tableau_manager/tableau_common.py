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
				"personalAccessTokenName": event_data['tableau_patname'],
				"personalAccessTokenSecret": event_data['tableau_patsecret'],
				"site": {
					"contentUrl": ""
					}
				}
			})
	response = requests.request("POST", url, headers=headers, data=payload, verify=event_data['verify_ssl'])
	site_id = response.json()['credentials']['site']['id']
	tableau_token = response.json()['credentials']['token']
	return tableau_token, site_id

def get_all_tableau_content(headers, event_data):

	tableau_pageSize = event_data['tableau_page_size']

	#if we received an email, assume that we need to filter tableau content by this
	#email (as tableau owner)
	if (event_data['owner_urlencoded'] != '') and (event_data['owner'] != ''):
		tableau_filter = '&filter=ownerEmail:eq:' + event_data['owner_urlencoded']
	else:
		tableau_filter = ''

	# WE NEED TO PAGINATE OUR API CALLS IN CASE THERE ARE MORE THAN ONE THOUSAND ITEMS
	tableau_content_page_number = 1  # 1-based, not zero based
	tableau_content_total_returned = 0
	tableau_content_done = False
	tableau_content = []
	while not(tableau_content_done):
		paging_parameters = 'pageSize={}&pageNumber={page_number}'.format(
			event_data['tableau_page_size'], 
			page_number=tableau_content_page_number)
		if event_data['content_searchtype'] == 'datasources':
			url = event_data['tableau_url'] + "/datasources?" + paging_parameters + tableau_filter
		if event_data['content_searchtype'] == 'workbooks':
			url = event_data['tableau_url'] + "/workbooks?" + paging_parameters + tableau_filter

		tableau_content_page = get_content(url, headers, event_data)
		if event_data['content_searchtype'] == 'datasources':
			paging = 'datasource'
		if event_data['content_searchtype'] == 'workbooks':
			paging = 'workbook'

		for item in tableau_content_page[event_data['content_searchtype']][paging]:
			tableau_content.append(item)
		total_available = int(tableau_content_page['pagination']['totalAvailable'])
		tableau_content_page_number += 1
		tableau_content_total_returned += int(tableau_pageSize)
		if(tableau_content_total_returned >= total_available):
			tableau_content_done = True

	return tableau_content

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

def update_one_connection(url, headers, event_data):

	print("\nupdating tableau connection: " + url)

	hostname = event_data['updated_hostname'] if event_data['updated_hostname'] else event_data['newhostname']
	username = event_data['updated_username'] if event_data['updated_username'] else event_data['newusername']
	password = event_data['updated_password'] if event_data['updated_password'] else event_data['newpassword']

	payload = json.dumps({
		"connection": {
		"serverAddress": hostname,
		"userName": username,
		"password": password,
		"embedPassword": "true",
		"queryTaggingEnabled": "true"
			}
		})

	print("\nupdating connection with this payload:\n" + payload)

	try:
		response = requests.request(
			'PUT', 
			url, 
			headers=headers, 
			data=payload, 
			verify=event_data['verify_ssl'])
		return response.json()

	except requests.exceptions.RequestException as e:
		raise SystemExit(e)