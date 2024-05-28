import requests
import json
import csv

import satori_common
import tableau_common


def analyze_content(event_data, satori_datastores, satori_users, tableau_content, writer):

	satori_headers = satori_common.satori_build_header(event_data['satori_token'])
	tableau_headers = tableau_common.build_tableau_header(event_data['tableau_token'])

	for content in tableau_content:

		content_name = content['name']

		#we need the content owner's email, and different tableau api versions have different data structures
		if event_data['tableau_api_version'] == '3.14':
			user_url = event_data['tableau_url'] + "/users/" + content['owner']['id']
			content_owner = tableau_common.get_one_user(
				user_url, tableau_headers, event_data)['user']['email']
		else:
			content_owner = content['owner']['name']

		content_id = content['id']

		#we need to catch project names that don't exist, because they are using Tableau 'Personal Spaces' feature
		#and Tableau Datasources don't use a location field, but Tableau Workbooks do.
		if event_data['content_searchtype'] == 'workbooks':
			if content['location']['type'] == 'PersonalSpace':
				content_project_name = "Personal Space"
			else:
		 		content_project_name = content['project']['name']
		else: 
			content_project_name = content['project']['name']

		#Tableau Datasources do not have content URL's, Tableau Workbooks do
		if event_data['content_searchtype'] == 'workbooks':
			content_url = content['webpageUrl']
		else:
			content_url = '********'

		#now let's get the data connections for each piece of content
		if event_data['content_searchtype'] == 'datasources':
			connections_url = event_data['tableau_url'] + "/datasources/" + content_id + "/connections"
		else:
			connections_url = event_data['tableau_url'] + "/workbooks/" + content_id + "/connections"

		content_connections = tableau_common.get_content(connections_url, tableau_headers, event_data)

		for connection in content_connections['connections']['connection']:

			connection_serverAddress = connection['serverAddress']


			# if we are looking at a Tableau Datasource, it's datasource name is at the highest level
			# else if Tableau Workbook, its datasource name is nested
			if event_data['content_searchtype'] == 'datasources':
				connection_name = content_name
			else:
				connection_name = connection['datasource']['name']

			connection_id = connection['id']
			governance = 'N/A'
			DoesTableauOwnerExistInSatori = 'False'

			#are we generating a report only for a single Satori Datastore ID?
			if event_data['mode'] == 'reportdatastore':
				if (connection_serverAddress == satori_datastores['satoriHostname']) or (connection_serverAddress == satori_datastores['hostname']):

					if connection_serverAddress == satori_datastores['satoriHostname']:
						governance = 'Governed'
					elif connection_serverAddress == satori_datastores['hostname']:
						governance = '😟  UnGoverned'
					elif connection_serverAddress.find("tableau.com")> 0:
						governance = "N/A"
					elif connection_serverAddress == '':
						governance = "N/A"
					elif connection_serverAddress == 'localhost':
						governance = "N/A"
					else:
						for search in event_data['dac_search']:
							if connection_serverAddress.find(search)> 0:
								governance = "Governed by a different Satori Datasource"
								break

					for user in satori_users:
						if user['email'] == content_owner:
							DoesTableauOwnerExistInSatori = 'True'
							break
						else:
							DoesTableauOwnerExistInSatori = 'False'



					writer.writerow({
					'Status': governance,
					'TableauProject': content_project_name,
					'ConnectionID': connection_id,
					'DatasourceName': connection_name,
					'DatasourceID': content_id,
					'WorkbookName': content_name,
					'TableauOwnerEmail': content_owner,
					'DoesTableauOwnerExistInSatori': DoesTableauOwnerExistInSatori,
					'ConnectionHostname': connection_serverAddress,
					'WorkbookURL': content_url
				})

			#or, are we generating a report only for a single email owner or the entire system?
			else:
				for search_string in event_data['dac_search']:
					if connection_serverAddress.find(search_string)> 0:
						governance = "Governed"
						break
					elif (connection_serverAddress == '') or (connection_serverAddress == 'localhost'):
						governance = "N/A"
					else:
						governance = "☹️  Ungoverned"

				for user in satori_users:
					if user['email'] == content_owner:
						DoesTableauOwnerExistInSatori = 'True'
						break
					else:
						DoesTableauOwnerExistInSatori = 'False'
					
				writer.writerow({
					'Status': governance,
					'TableauProject': content_project_name,
					'ConnectionID': connection_id,
					'DatasourceName': connection_name,
					'DatasourceID': content_id,
					'WorkbookName': content_name,
					'TableauOwnerEmail': content_owner,
					'DoesTableauOwnerExistInSatori': DoesTableauOwnerExistInSatori,
					'ConnectionHostname': connection_serverAddress,
					'WorkbookURL': content_url
				})

			print(governance + ' Connection: ' + content_name + ' (' + content_id + ')')
			print('owner: ' + content_owner)
			print('DoesTableauOwnerExistInSatori? ' + DoesTableauOwnerExistInSatori)
			print('project: ' + content_project_name)
			print('hostname: ' + connection_serverAddress)
			print('datasource name: ' + connection_name)
			print('content ID: ' + content_id)
			print('connection ID: ' + connection_id)
			print('')


def analyze_connections(event_data):

	satori_headers = satori_common.satori_build_header(event_data['satori_token'])
	
	if event_data['mode'] == 'reportdatastore':
		satori_datastores = satori_common.get_one_datastore(satori_headers, event_data)
	else:
		satori_datastores = satori_common.get_all_datastores(satori_headers, event_data)

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
		print('ANALYZING TABLEAU DATASOURCE CONNECTIONS')
		
		event_data['content_searchtype'] = 'datasources'
		tableau_datasources = tableau_common.get_all_tableau_content(tableau_headers, event_data)

		if len(tableau_datasources) > 0:
			datasources_analyzed = analyze_content(
				event_data, 
				satori_datastores, 
				satori_users, 
				tableau_datasources, 
				writer)

		print("_____________________________________________________________")
		print('ANALYZING TABLEAU WORKBOOK CONNECTIONS')

		event_data['content_searchtype'] = 'workbooks'
		tableau_workbooks = tableau_common.get_all_tableau_content(tableau_headers, event_data)

		if len(tableau_workbooks) > 0:
			workbooks_analyzed = analyze_content(
				event_data, 
				satori_datastores, 
				satori_users, 
				tableau_workbooks, 
				writer)

		print('\n\n' + event_data['filename'] + ' file has been generated')

