import requests
import json
import csv

import satori_common
import tableau_handlers

def govern_single_connection(event_data):

	tableau_headers = tableau_handlers.build_tableau_header(event_data['tableau_token'])

	#first, we need the tableau content owner email address to perform a lookup in Satori

	content_url = event_data['tableau_url'] + "/datasources/" + event_data['content_id']

	tableau_content = tableau_handlers.get_content(content_url, tableau_headers)

	print("\n\nfound this tableau content: " + str(tableau_content))


	content_name = '_' + tableau_content['datasource']['name'].lower().replace(' ', '_')
	content_email = tableau_content['datasource']['owner']['name']
	print("\n\ntableau content owner is: " + content_email)
	
	satori_headers = satori_common.satori_build_header(event_data['satori_token'])

	satori_datastore = satori_common.get_one_datastore(
		satori_headers, 
		event_data['satori_api_hostname'],
		event_data['datastore_id']
		)

	print("\n\nfound this satori datastore: " + str(satori_datastore))

	satori_user = satori_common.get_one_user(
		satori_headers, 
		event_data['satori_account_id'],
		event_data['satori_api_hostname'],
		email=content_email)

	if satori_user is None:
		print("no Satori user found, aborting")
	else:
		print("found this satori user: " + str(satori_user))
		satori_user_id = satori_user['id']

		#generate a PAT in Satori for the user email
		
		newpat_result = satori_common.generate_satori_pat(
			satori_headers, 
			event_data['satori_api_hostname'], 
			satori_user_id,
			event_data['satori_new_pat_name'])

		if newpat_result[1] in (400, 409):
			print("Duplicate Pat Found or PAT Format invalid")

		else:
			print("\nreceived a new satori PAT: " + str(newpat_result[0]))
			
			pat = newpat_result[0]

			connection_url = event_data['tableau_url'] + "/datasources/" + event_data['content_id'] + "/connections/" + event_data['connection_id']


			updated_content = tableau_handlers.update_one_connection(
				connection_url, 
				tableau_headers, 
				satori_datastore['satoriHostname'], 		
				pat['tokenName'],
				pat['token'])

			print('\nconnection updated with this response: \n')
			print(updated_content)

def manual_single_connection(event_data):

	headers = tableau_handlers.build_tableau_header(event_data['tableau_token'])

	content_url =  "{}/datasources/{}/connections/{}".format(
		event_data['tableau_url'],
		event_data['content_id'],
		event_data['connection_id'])

	updated_content = tableau_handlers.update_one_connection(
		content_url, 
		headers, 
		event_data['newhostname'], 
		event_data['newusername'],
		event_data['newpassword'])

	print('\n\nconnection updated with this response: \n')
	print(updated_content)

			