import requests
import json
import csv

import satori_common
import tableau_common

def analyze_connections(event_data, filename):

	satori_headers = satori_common.satori_build_header(event_data['satori_token'])
	
	satori_datastores = satori_common.get_all_datastores(
		satori_headers, 
		event_data)
	
	satori_users = satori_common.get_all_users(
		satori_headers,
		event_data)

	tableau_token = event_data['tableau_token']
	dac_search = event_data['dac_search']

	tableau_headers = tableau_common.build_tableau_header(tableau_token)

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
		print("FINDING DATASOURCE CONNECTIONS")

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
				datasource_project = datasource['project']['name']

				print("datasource \"" + datasource_name + "\": getting connections")
				print("owner is: " + datasource_owner)

				url = event_data['tableau_url'] + "/datasources/" + datasource_id + "/connections"
				datasource_connections = tableau_common.get_content(url, tableau_headers, event_data)

				for connection in datasource_connections['connections']['connection']:

					connection_serverAddress = connection['serverAddress']
					connection_id = connection['id']

					print("connection hostname is: " + connection_serverAddress)
					
					if connection_serverAddress == 'localhost' or connection_serverAddress == '':
						governance = 'N/A'
					else:
						for search_string in event_data['dac_search']:
							if connection_serverAddress.find(search_string)> 0:
								governance = "Governed"
								break
							else:
								governance = "☹️ Ungoverned"

					for datastore in satori_datastores:
						if datastore['hostname'] == connection_serverAddress:
							ShouldBeSatoriHostname = datastore['satoriHostname']
							break
						elif datastore['satoriHostname'] == connection_serverAddress:
							ShouldBeSatoriHostname = '😀 Already updated to Satori Hostname '
							break
						else:
							ShouldBeSatoriHostname = '☹️ No Satori Hostname Found!'

					for user in satori_users:
						if user['email'] == datasource_owner:
							DoesTableauOwnerExistInSatori = 'True'
							break
						else:
							DoesTableauOwnerExistInSatori = 'False'

					writer.writerow({
						'Status': governance,
						'TableauProject': datasource_project,
						'ConnectionID': connection_id,
						'DatasourceName': datasource_name,
						'DatasourceID': datasource_id,
						'WorkbookName': '*****',
						'TableauOwnerEmail': datasource_owner,
						'DoesTableauOwnerExistInSatori': DoesTableauOwnerExistInSatori,
						'ConnectionHostname': connection_serverAddress,
						"ShouldBeSatoriHostname": ShouldBeSatoriHostname,
						'WorkbookURL': '*****'
						})

		print("FINDING WORKBOOK CONNECTIONS")

		tableau_workbooks = tableau_common.get_all_tableau_workbooks(tableau_headers, event_data)

		if len(tableau_workbooks) > 0:

			for workbook in tableau_workbooks:

				workbook_name = workbook['name']
				workbook_owner = workbook['owner']['name']
				workbook_id = workbook['id']
				workbook_url = workbook['webpageUrl']
				workbook_project_name = workbook['project']['name']

				print("workbook \"" + workbook_name + "\": getting connections")
				print("owner is: " + workbook_owner)

				url = event_data['tableau_url'] + "/workbooks/" + workbook_id + "/connections"
				workbook_connections = tableau_common.get_content(url, tableau_headers, event_data)

				for connection in workbook_connections['connections']['connection']:
					
					connection_serverAddress = connection['serverAddress']
					connection_id = connection['id']
					connection_datasource_name = connection['datasource']['name']
					connection_datasource_id = connection['datasource']['id']
					connection_type = connection['type']

					print("connection hostname is: " + connection_serverAddress)

					#NOTE: Workbooks that use published datasources show up as 'sqlproxy'
					#I.E. it's a reference to a separate datasource that we have already caught up above
					if connection_type != 'sqlproxy':
						for search_string in dac_search:
							if connection_serverAddress.find(search_string) > 0:
								governance = "Governed"
								break
							else:
								governance = "Ungoverned"

						if connection_serverAddress == '':
							governance = 'N/A'


						for datastore in satori_datastores:
							if datastore['hostname'] == connection_serverAddress:
								ShouldBeSatoriHostname = datastore['satoriHostname']
								break
							elif datastore['satoriHostname'] == connection_serverAddress:
								ShouldBeSatoriHostname = '😀 Already updated to Satori Hostname '
								break
							else:
								ShouldBeSatoriHostname = '☹️ No Satori Hostname Found!'

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
							"ShouldBeSatoriHostname": ShouldBeSatoriHostname,
							'WorkbookURL': workbook_url
							})
		print('\n\n' + event_data['filename'] + ' file has been generated')