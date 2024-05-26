import requests
import json
import csv

import satori_common
import tableau_common


def analyze_content(event_data, satori_datastores, satori_users, tableau_content, writer):

	satori_headers = satori_common.satori_build_header(event_data['satori_token'])
	tableau_headers = tableau_common.build_tableau_header(event_data['tableau_token'])

	for datasource in tableau_content:

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

			#are we generating a report only for a single Satori Datastore ID?
			governance = 'N/A'
			DoesTableauOwnerExistInSatori = 'False'

			if event_data['mode'] == 'reportdatastore':
				if (connection_serverAddress == satori_datastores['satoriHostname']) or (connection_serverAddress == satori_datastores['hostname']):

					if connection_serverAddress == satori_datastores['satoriHostname']:
						governance = 'Governed'
					elif connection_serverAddress == satori_datastores['hostname']:
						governance = '😟 UnGoverned'

					for user in satori_users:
						if user['email'] == datasource_owner:
							DoesTableauOwnerExistInSatori = 'True'
							break
						else:
							DoesTableauOwnerExistInSatori = 'False'

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
						'WorkbookURL': '*****'
					})

			#or, are we generating a report only for a single email owner, or, the entire system?
			else:
				for search_string in event_data['dac_search']:
					if connection_serverAddress.find(search_string)> 0:
						governance = "Governed"
						break
					elif (connection_serverAddress == '') or (connection_serverAddress == 'localhost'):
						governance = "N/A"
					else:
						governance = "☹️ Ungoverned"

				for user in satori_users:
					if user['email'] == datasource_owner:
						DoesTableauOwnerExistInSatori = 'True'
						break
					else:
						DoesTableauOwnerExistInSatori = 'False'
						
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
					'WorkbookURL': '*****'
				})


			print(governance + ' Datasource Connection: ' + datasource_name + ' (' + datasource_id + ')')
			print('owner: ' + datasource_owner)
			print('DoesTableauOwnerExistInSatori? ' + DoesTableauOwnerExistInSatori)
			print('project: ' + datasource_project_name)
			print('hostname: ' + connection_serverAddress)
			print('content ID: ' + datasource_id)
			print('connection ID: ' + connection_id)
			print('')






def analyze_connections(event_data):

	satori_headers = satori_common.satori_build_header(event_data['satori_token'])
	
	if event_data['mode'] == 'reportdatastore':
		satori_datastores = satori_common.get_one_datastore(
			satori_headers, 
			event_data['satori_api_hostname'],
			event_data['datastore_id']
			)
	else:
		satori_datastores = satori_common.get_all_datastores(
			satori_headers, 
			event_data
			)

	satori_users = satori_common.get_all_users(
		satori_headers, 
		event_data)

	tableau_headers = tableau_common.build_tableau_header(event_data['tableau_token'])

	with open(event_data['filename'], 'w', newline='\n') as file:			
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
		"WorkbookURL"]

		writer = csv.DictWriter(
			file, 
			fieldnames=fieldheaders, 
			delimiter=separator, 
			quotechar=quote, 
			quoting=csv.QUOTE_NONNUMERIC)

		writer.writeheader()

		print("_____________________________________________________________")
		if event_data['mode'] == 'reportdatastore':
			print('ANALYZING TABLEAU DATASOURCE CONNECTIONS FOR SATORI DATASTORE: \"' + satori_datastores['name'] + '\"')
		else:
			print('ANALYZING TABLEAU DATASOURCE CONNECTIONS')
		

		event_data['content_searchtype'] = 'datasources'
		tableau_datasources = tableau_common.get_all_tableau_content(tableau_headers, event_data)

		if len(tableau_datasources) > 0:

			newrows = analyze_content(event_data, satori_datastores, satori_users, tableau_datasources, writer)

		print("_____________________________________________________________")
		if event_data['mode'] == 'reportdatastore':
			print('ANALYZING TABLEAU WORKBOOK CONNECTIONS FOR SATORI DATASTORE: \"' + satori_datastores['name'] + '\"')
		else:
			print('ANALYZING TABLEAU WORKBOOK CONNECTIONS')

		event_data['content_searchtype'] = 'workbooks'
		tableau_workbooks = tableau_common.get_all_tableau_content(tableau_headers, event_data)

		if len(tableau_workbooks) > 0:

			for workbook in tableau_workbooks:

				workbook_name = workbook['name']
				workbook_owner = workbook['owner']['name']
				workbook_id = workbook['id']

				if workbook['location']['type'] == 'PersonalSpace':
					workbook_project_name = "Personal Space"
				else:
	 				workbook_project_name = workbook['project']['name']
				workbook_url = workbook['webpageUrl']

				url = event_data['tableau_url'] + "/workbooks/" + workbook_id + "/connections"
				workbook_connections = tableau_common.get_content(url, tableau_headers, event_data)

				for connection in workbook_connections['connections']['connection']:
					
					connection_serverAddress = connection['serverAddress']
					connection_id = connection['id']
					connection_datasource_name = connection['datasource']['name']
					connection_datasource_id = connection['datasource']['id']

					governance = 'N/A'
					DoesTableauOwnerExistInSatori = 'False'
					#are we generating a report only for a single Satori Datastore ID?
					if event_data['mode'] == 'reportdatastore':
						if (connection_serverAddress == satori_datastores['satoriHostname']) or (connection_serverAddress == satori_datastores['hostname']):

							if connection_serverAddress == satori_datastores['satoriHostname']:
								governance = 'Governed'
							elif connection_serverAddress == satori_datastores['hostname']:
								governance = '😟 UnGoverned'
							elif connection_serverAddress.find("tableau.com")> 0:
								governance = "N/A"
							elif connection_serverAddress == '':
								governance = "N/A"
							elif connection_serverAddress == 'localhost':
								governance = "N/A"


							for user in satori_users:
								if user['email'] == workbook_owner:
									DoesTableauOwnerExistInSatori = 'True'
									break
								else:
									DoesTableauOwnerExistInSatori = 'False'

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
								'WorkbookURL': workbook_url
							})

					#or, are we generating a report only for a single email owner, or, the entire system?
					else:
						for search_string in event_data['dac_search']:
							if connection_serverAddress.find(search_string)> 0:
								governance = "Governed"
								break
							elif connection_serverAddress.find("tableau.com")> 0:
								governance = "N/A"
							elif connection_serverAddress == '':
								governance = "N/A"
							elif connection_serverAddress == 'localhost':
								governance = "N/A"
							else:
								governance = "☹️ Ungoverned"

						for user in satori_users:
							if user['email'] == workbook_owner:
								DoesTableauOwnerExistInSatori = 'True'
								break
							else:
								DoesTableauOwnerExistInSatori = 'False'

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
							'WorkbookURL': workbook_url
							})

					
					print(governance + ' Workbook Connection: ' + workbook_name + ' (' + workbook_id + ')')
					print('owner: ' + workbook_owner)
					print('DoesTableauOwnerExistInSatori? ' + DoesTableauOwnerExistInSatori)
					print('project: ' + workbook_project_name)
					print('hostname: ' + connection_serverAddress)
					print('workbook url: ' + workbook_url)
					print('content ID: ' + workbook_id)
					print('connection ID: ' + connection_id)

					print('')



		print('\n\n' + event_data['filename'] + ' file has been generated')