import requests
import json
import csv

import satori_common
import tableau_common

def govern_single_connection(event_data):

	tableau_headers = tableau_common.build_tableau_header(event_data['tableau_token'])
	satori_headers = satori_common.satori_build_header(event_data['satori_token'])
	satori_api_hostname = event_data['satori_api_hostname']

	#first, we need the tableau content owner email address to perform a lookup in Satori
	content_url = event_data['tableau_url'] + "/datasources/" + event_data['content_id']
	tableau_content = tableau_common.get_content(content_url, tableau_headers, event_data)
	print("\n\nfound this tableau content: " + str(tableau_content))

	content_name = '_' + tableau_content['datasource']['name'].lower().replace(' ', '_')
	#different Tableau API versions have different ways to get the owner name/email
	
	if event_data['tableau_api_version'] == '3.14':
		url = event_data['tableau_url'] + "/users/" + tableau_content['datasource']['owner']['id']
		content_email = tableau_common.get_one_user(
			url, tableau_headers, event_data)['user']['email']
	else:
		content_email = tableau_content['owner']['name']	
	print("\n\ntableau content owner is: " + content_email)
	

	satori_datastore = satori_common.get_one_datastore(satori_headers, event_data)
	print("\n\nfound this satori datastore: " + str(satori_datastore))
	event_data['content_owner'] = content_email
	
	#now verify this tableau content owner exists in Satori
	satori_user = satori_common.get_one_user(satori_headers, event_data)

	if satori_user is None:
		print("no Satori user found, aborting")
	else:
		print("\n\nfound this satori user: " + str(satori_user))

		#generate a PAT in Satori with the satori user id
		event_data['satori_user_id'] = satori_user['id']
		newpat_result = satori_common.generate_satori_pat(satori_headers, event_data)

		if newpat_result[1] in (400, 409):
			print("Duplicate Pat Found or PAT Format invalid")

		else:
			print("\nreceived a new satori PAT: " + str(newpat_result[0]))
			
			event_data['updated_hostname'] = satori_datastore['satoriHostname']
			satori_pat = newpat_result[0]
			event_data['updated_username'] = satori_pat['tokenName']
			event_data['updated_password'] = satori_pat['token']

			connection_url = event_data['tableau_url'] + "/datasources/" + event_data['content_id'] + "/connections/" + event_data['connection_id']

			updated_content = tableau_common.update_one_connection(
				connection_url, 
				tableau_headers,
				event_data)

			print('\nconnection updated with this response: \n')
			print(updated_content)

def manual_single_connection(event_data):

	#100% manual mode, we need:
	#hostname, username, password, tableau content_id and tableau connection_id 

	headers = tableau_common.build_tableau_header(event_data['tableau_token'])

	event_data['updated_hostname'] = event_data['newhostname']
	event_data['updated_username'] = event_data['newusername']
	event_data['updated_password'] = event_data['newpassword']

	content_url =  "{}/datasources/{}/connections/{}".format(
		event_data['tableau_url'],
		event_data['content_id'],
		event_data['connection_id'])

	updated_content = tableau_common.update_one_connection(
		content_url, 
		headers, 
		event_data)

	print('\n\nconnection updated with this response: \n')
	print(updated_content)

			