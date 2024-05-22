import requests
import json
import csv

import satori_common
import tableau_handlers

def govern_user_connections(event_data):


	# THIS METHOD IS NOT FINISHED, INERT, HARMLESS
	# WORK TBD

	tableau_headers = tableau_handlers.build_tableau_header(event_data['tableau_token'])
	satori_headers = satori_common.satori_build_header(event_data['satori_token'])

	#first, verify the content owner exists in Satori
	satori_user = satori_common.get_one_user(
			satori_headers, 
			event_data['satori_account_id'],
			event_data['satori_api_hostname'],
			event_data['owner'])

	if satori_user is None:
		print("no Satori user found, aborting")
	else:
		print("found this satori user: " + str(satori_user) + '\n')
		satori_user_id = satori_user['id']

		tableau_datasources = tableau_handlers.get_all_tableau_datasources(tableau_headers, event_data)

		if len(tableau_datasources) > 0:

			for datasource in tableau_datasources:

				datasource_name = datasource['name']
				datasource_owner = datasource['owner']['name']
				datasource_id = datasource['id']
				datasource_project_name = datasource['project']['name']

				url = event_data['tableau_url'] + "/datasources/" + datasource_id + "/connections"
				datasource_connections = tableau_handlers.get_content(url, tableau_headers)

				for connection in datasource_connections['connections']['connection']:

					connection_serverAddress = connection['serverAddress']
					connection_id = connection['id']

					print('name: ' + datasource_name)
					print('owner: ' + datasource_owner)
					print('project: ' + datasource_project_name)
					print('datasource ID: ' + datasource_id)
					print('connection ID: ' + connection_id)
					print('hostname: ' + connection_serverAddress)
					print('')

		tableau_workbooks = tableau_handlers.get_all_tableau_workbooks(tableau_headers, event_data)

		if len(tableau_workbooks) > 0:

			for workbook in tableau_workbooks:

				workbook_name = workbook['name']
				workbook_owner = workbook['owner']['name']
				workbook_id = workbook['id']
				workbook_project_name = workbook['project']['name']
				workbook_url = workbook['webpageUrl']

				url = event_data['tableau_url'] + "/workbooks/" + workbook_id + "/connections"
				workbook_connections = tableau_handlers.get_content(url, tableau_headers)

				for connection in workbook_connections['connections']['connection']:
					
					connection_serverAddress = connection['serverAddress']
					connection_id = connection['id']
					connection_datasource_name = connection['datasource']['name']
					connection_datasource_id = connection['datasource']['id']

					print('name: ' + workbook_name)
					print('owner: ' + workbook_owner)
					print('project: ' + workbook_project_name)
					print('workbook url: ' + workbook_url)
					print('datasource ID: ' + connection_datasource_id)
					print('connection ID: ' + connection_id)
					print('hostname: ' + connection_serverAddress)
					print('')

			
