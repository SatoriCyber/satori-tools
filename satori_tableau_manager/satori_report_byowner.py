import requests
import json
import csv

import satori_common
import tableau_common

def analyze_owner_connections(event_data, filename):

	tableau_headers = tableau_common.build_tableau_header(event_data['tableau_token'])

	with open(filename, 'w', newline='\n') as file:			
		separator = ','
		quote = '"'
		fieldheaders = [
		"Status", 
		"DatasourceName", 
		"DatasourceID", 
		"TableauOwnerEmail", 
		"TableauProject", 
		"WorkbookName",
		"ConnectionHostname",
		"ConnectionID",
		"WorkbookURL"
		]

		writer = csv.DictWriter(
			file, 
			fieldnames=fieldheaders, 
			delimiter=separator, 
			quotechar=quote, 
			quoting=csv.QUOTE_NONNUMERIC)

		writer.writeheader()

		print("_____________________________________________________________")
		print('FINDING TABLEAU DATASOURCE CONNECTIONS FOR OWNER: \"' + event_data['owner'] + '\"')

		tableau_datasources = tableau_common.get_all_tableau_datasources(tableau_headers, event_data)

		if len(tableau_datasources) > 0:

			for datasource in tableau_datasources:

				datasource_name = datasource['name']

				if event_data['tableau_api_version'] == '3.14':
					url = event_data['tableau_url'] + "/users/" + datasource['owner']['id']
					datasource_owner = tableau_common.get_one_user(
						url, tableau_headers, event_data)['user']['email']
				else:
					datasource_owner = datasource['owner']['name']

				datasource_id = datasource['id']
				datasource_project_name = datasource['project']['name']

				url = event_data['tableau_url'] + "/datasources/" + datasource_id + "/connections"
				datasource_connections = tableau_common.get_content(url, tableau_headers, event_data)

				for connection in datasource_connections['connections']['connection']:

					connection_serverAddress = connection['serverAddress']
					connection_id = connection['id']

					for search_string in event_data['dac_search']:
						if connection_serverAddress.find(search_string)> 0:
							governance = "Governed"
							break
						else:
							governance = "😟 Ungoverned"

					print(governance + ' Datasource Connection: ' + datasource_name + ' (' + datasource_id + ')')
					print('owner: ' + datasource_owner)
					print('project: ' + datasource_project_name)
					print('hostname: ' + connection_serverAddress)
					print('datasource ID: ' + datasource_id)
					print('connection ID: ' + connection_id)
					print('')

					writer.writerow({
						'Status': governance,
						'DatasourceName': datasource_name,
						'DatasourceID': datasource_id,
						'WorkbookName': '*****',
						'TableauOwnerEmail': datasource_owner,
						'TableauProject': datasource_project_name,
						'ConnectionHostname': connection_serverAddress,
						'ConnectionID': connection_id,
						'WorkbookURL': '*****'
						})


		print("_____________________________________________________________")
		print('FINDING TABLEAU WORKBOOK CONNECTIONS FOR OWNER: \"' + event_data['owner'] + '\"')

		tableau_workbooks = tableau_common.get_all_tableau_workbooks(tableau_headers, event_data)

		if len(tableau_workbooks) > 0:

			for workbook in tableau_workbooks:

				workbook_name = workbook['name']
				workbook_owner = workbook['owner']['name']
				workbook_id = workbook['id']
				workbook_project_name = workbook['project']['name']
				workbook_url = workbook['webpageUrl']

				url = event_data['tableau_url'] + "/workbooks/" + workbook_id + "/connections"
				workbook_connections = tableau_common.get_content(url, tableau_headers, event_data)

				for connection in workbook_connections['connections']['connection']:
					
					connection_serverAddress = connection['serverAddress']
					connection_id = connection['id']
					connection_datasource_name = connection['datasource']['name']
					connection_datasource_id = connection['datasource']['id']

					for search_string in event_data['dac_search']:
						if connection_serverAddress.find(search_string)> 0:
							governance = "Governed"
							break
						else:
							governance = "😟 Ungoverned"

					print(governance + ' Workbook Connection: ' + workbook_name + ' (' + workbook_id + ')')
					print('owner: ' + workbook_owner)
					print('project: ' + workbook_project_name)
					print('hostname: ' + connection_serverAddress)
					print('workbook url: ' + workbook_url)
					print('workbook ID: ' + workbook_id)
					print('connection ID: ' + connection_id)

					print('')

					writer.writerow({
						'Status': governance,
						'DatasourceName': connection_datasource_name,
						'DatasourceID': connection_datasource_id,
						'WorkbookName': workbook_name,
						'TableauOwnerEmail': workbook_owner,
						'TableauProject': workbook_project_name,
						'ConnectionHostname': connection_serverAddress,
						'ConnectionID': connection_id,
						'WorkbookURL': workbook_url
						})

		print('\n\n' + event_data['filename'] + ' file has been generated')