import requests
import json
import csv

import satori_common
import tableau_handlers

def analyze_datastore_connections(event_data, filename):

	satori_headers = satori_common.satori_build_header(event_data['satori_token'])
	
	satori_datastore = satori_common.get_one_datastore(
		satori_headers, 
		event_data['satori_api_hostname'],
		event_data['datastore_id']
		)
	
	satori_users = satori_common.get_all_users(
		satori_headers, 
		event_data)

	headers = tableau_handlers.build_tableau_header(event_data['tableau_token'])


	with open(filename, 'w', newline='\n') as file:			
		separator = ','
		quote = '"'
		fieldheaders = [
		"Status", 
		"TableauProject", 
		"ConnectionID", 
		"DatasourceName", 
		"DatasourceID", 
		"WorkbookName", 
		"TableauOwnerEmail", 
		"DoesTableauOwnerExistInSatori",
		"ConnectionHostname",
		"ShouldBeSatoriHostname",
		"WorkbookURL"]

		writer = csv.DictWriter(
			file, 
			fieldnames=fieldheaders, 
			delimiter=separator, 
			quotechar=quote, 
			quoting=csv.QUOTE_NONNUMERIC)

		writer.writeheader()

		print("_____________________________________________________________")
		print('FINDING TABLEAU DATASOURCE CONNECTIONS FOR SATORI DATASTORE: \"' + satori_datastore['name'] + '\"')

		project = ''
		tableau_datasources = tableau_handlers.get_all_tableau_datasources(headers, event_data)

		if len(tableau_datasources) > 0:

			for datasource in tableau_datasources:

				datasource_name = datasource['name']
				datasource_owner = datasource['owner']['name']
				datasource_id = datasource['id']
				datasource_project_name = datasource['project']['name']

				url = event_data['tableau_url'] + "/datasources/" + datasource_id + "/connections"
				datasource_connections = tableau_handlers.get_content(url, headers)

				for connection in datasource_connections['connections']['connection']:

					connection_serverAddress = connection['serverAddress']
					connection_id = connection['id']

					if (connection_serverAddress == satori_datastore['satoriHostname']) or (connection_serverAddress == satori_datastore['hostname']):

						if connection_serverAddress == satori_datastore['satoriHostname']:
							governance = 'Governed'
						if connection_serverAddress == satori_datastore['hostname']:
							governance = '😟 UnGoverned'
						
						if satori_datastore['hostname'] == connection_serverAddress:
							ShouldBeSatoriHostname = satori_datastore['satoriHostname']
						if satori_datastore['satoriHostname'] == connection_serverAddress:
							ShouldBeSatoriHostname = 'Already updated to Satori Hostname'

						for user in satori_users:
							if user['email'] == datasource_owner:
								DoesTableauOwnerExistInSatori = 'True'
								break
							else:
								DoesTableauOwnerExistInSatori = 'False'

						print(governance + ' Datasource Connection: ' + datasource_name + ' (' + datasource_id + ')')
						print('owner: ' + datasource_owner)
						print('DoesTableauOwnerExistInSatori? ' + DoesTableauOwnerExistInSatori)
						print('project: ' + datasource_project_name)
						print('hostname: ' + connection_serverAddress)
						print('should be hostname: ' + ShouldBeSatoriHostname)
						print('datasource ID: ' + datasource_id)
						print('connection ID: ' + connection_id)
						print('')

						writer.writerow({
							'Status': governance,
							'TableauProject': datasource_project_name,
							'ConnectionID': connection_id,
							'DatasourceName': datasource_name,
							'DatasourceID': datasource_id,
							'WorkbookName': '*****',
							'TableauOwnerEmail': datasource_owner,
							'DoesTableauOwnerExistInSatori': DoesTableauOwnerExistInSatori,
							'ConnectionHostname': connection_serverAddress,
							'ShouldBeSatoriHostname': ShouldBeSatoriHostname,
							'WorkbookURL': '*****'
							})


		print("_____________________________________________________________")
		print('FINDING TABLEAU WORKBOOK CONNECTIONS FOR SATORI DATASTORE: \"' + satori_datastore['name'] + '\"')

		tableau_workbooks = tableau_handlers.get_all_tableau_workbooks(headers, event_data)

		if len(tableau_workbooks) > 0:

			for workbook in tableau_workbooks:

				workbook_name = workbook['name']
				workbook_owner = workbook['owner']['name']
				workbook_id = workbook['id']
				workbook_project_name = workbook['project']['name']
				workbook_url = workbook['webpageUrl']

				url = event_data['tableau_url'] + "/workbooks/" + workbook_id + "/connections"
				workbook_connections = tableau_handlers.get_content(url, headers)

				for connection in workbook_connections['connections']['connection']:
					
					connection_serverAddress = connection['serverAddress']
					connection_id = connection['id']
					connection_datasource_name = connection['datasource']['name']
					connection_datasource_id = connection['datasource']['id']

					if (connection_serverAddress == satori_datastore['satoriHostname']) or (connection_serverAddress == satori_datastore['hostname']):

						if connection_serverAddress == satori_datastore['satoriHostname']:
							governance = 'Governed'
						if connection_serverAddress == satori_datastore['hostname']:
							governance = '😟 UnGoverned'
						
						if satori_datastore['hostname'] == connection_serverAddress:
							ShouldBeSatoriHostname = satori_datastore['satoriHostname']
						if satori_datastore['satoriHostname'] == connection_serverAddress:
							ShouldBeSatoriHostname = 'Already updated to Satori Hostname'

						for user in satori_users:
							if user['email'] == datasource_owner:
								DoesTableauOwnerExistInSatori = 'True'
								break
							else:
								DoesTableauOwnerExistInSatori = 'False'

						print(governance + ' Workbook Connection: ' + workbook_name + ' (' + workbook_id + ')')
						print('owner: ' + workbook_owner)
						print('DoesTableauOwnerExistInSatori? ' + DoesTableauOwnerExistInSatori)
						print('project: ' + workbook_project_name)
						print('hostname: ' + connection_serverAddress)
						print('should be hostname: ' + ShouldBeSatoriHostname)
						print('workbook url: ' + workbook_url)
						print('workbook ID: ' + workbook_id)
						print('connection ID: ' + connection_id)

						print('')

						writer.writerow({
							'Status': governance,
							'TableauProject': workbook_project_name,
							'ConnectionID': connection_id,
							'DatasourceName': connection_datasource_name,
							'DatasourceID': connection_datasource_id,
							'WorkbookName': workbook_name,
							'TableauOwnerEmail': workbook_owner,
							'DoesTableauOwnerExistInSatori': DoesTableauOwnerExistInSatori,
							'ConnectionHostname': connection_serverAddress,
							"ShouldBeSatoriHostname": ShouldBeSatoriHostname,
							'WorkbookURL': workbook_url
							})

		print("\n\nCSV File has been generated")