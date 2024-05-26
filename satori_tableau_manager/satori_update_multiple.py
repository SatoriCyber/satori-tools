import requests
import json
import csv

import satori_common
import tableau_common

def govern_user_connections(event_data):

	tableau_headers = tableau_common.build_tableau_header(event_data['tableau_token'])
	satori_headers = satori_common.satori_build_header(event_data['satori_token'])

	#first, verify the content owner and Satori Datastore both exist in Satori
	satori_user = satori_common.get_one_user(
			satori_headers, 
			event_data['satori_account_id'],
			event_data['satori_api_hostname'],
			event_data['owner'],
			event_data)

	satori_datastore = satori_common.get_one_datastore(
		satori_headers, 
		event_data['satori_api_hostname'],
		event_data['datastore_id']
		)


	if (satori_user is None) or (satori_datastore is None):
		print("no Satori user or Satori Datastore found, aborting")

	else:

		#make an empty Satori PAT. If we find any one single connection to update
		#we will fill in this empty PAT and then check for its existence for any 2nd or Nth update

		satori_pat = []

		print("\n\nfound this satori user: " + str(satori_user) + '\n\nNow finding any Tableau content for this user')
		satori_user_id = satori_user['id']

		tableau_datasources = tableau_common.get_all_tableau_datasources(tableau_headers, event_data)
		tableau_workbooks = tableau_common.get_all_tableau_workbooks(tableau_headers, event_data)

		if (len(tableau_datasources) == 0) and (len(tableau_workbooks) == 0):
			print("no content found for this user, aborting without generating a Satori PAT")
		else:

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

					datasource_url = event_data['tableau_url'] + "/datasources/" + datasource_id + "/connections"
					datasource_connections = tableau_common.get_content(datasource_url, tableau_headers, event_data)

					for connection in datasource_connections['connections']['connection']:

						connection_serverAddress = connection['serverAddress']
						connection_id = connection['id']

						print('name: ' + datasource_name)
						print('owner: ' + datasource_owner)
						print('project: ' + datasource_project_name)
						print('datasource ID: ' + datasource_id)
						print('connection ID: ' + connection_id)
						print('hostname: ' + connection_serverAddress)

						if (connection_serverAddress == satori_datastore['hostname']):
							print('FOUND A HOSTNAME TO UPDATE')

							connection_url = event_data['tableau_url'] + "/datasources/" + datasource_id + "/connections/" + connection_id

							if len(satori_pat) == 0:
								#generate a PAT in Satori for the user email
								satori_pat_temp = satori_common.generate_satori_pat(
									satori_headers, 
									event_data['satori_api_hostname'], 
									satori_user_id,
									event_data['satori_newpatname'],
									event_data)
								
								if satori_pat_temp[1] in (400, 409):
									print("Duplicate Pat Found or PAT Format invalid")
								else:
									print("\nreceived a new satori PAT: " + str(satori_pat_temp[0]))
									satori_pat = satori_pat_temp[0]

									updated_content = tableau_common.update_one_connection(
										connection_url, 
										tableau_headers, 
										satori_datastore['satoriHostname'], 		
										satori_pat['tokenName'],
										satori_pat['token'],
										event_data)

									print('\nconnection updated with this response: \n')
									print(updated_content)

							else:
								print("reusing existing PAT: " + satori_pat['tokenName'])
								updated_content = tableau_common.update_one_connection(
									connection_url, 
									tableau_headers, 
									satori_datastore['satoriHostname'], 		
									satori_pat['tokenName'],
									satori_pat['token'],
									event_data)

								print('\nconnection updated with this response: \n')
								print(updated_content)




			#tableau_workbooks = tableau_common.get_all_tableau_workbooks(tableau_headers, event_data)

			if len(tableau_workbooks) > 0:

				for workbook in tableau_workbooks:

					workbook_name = workbook['name']
					workbook_owner = workbook['owner']['name']
					workbook_id = workbook['id']
					workbook_project_name = workbook['project']['name']
					workbook_url = workbook['webpageUrl']

					workbook_url = event_data['tableau_url'] + "/workbooks/" + workbook_id + "/connections"
					workbook_connections = tableau_common.get_content(workbook_url, tableau_headers, event_data)

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
			
						if (connection_serverAddress == satori_datastore['hostname']):
							print('FOUND A HOSTNAME TO UPDATE')

							if len(satori_pat) == 0:
								#generate a PAT in Satori for the user email
								satori_pat_temp = satori_common.generate_satori_pat(
									satori_headers, 
									event_data['satori_api_hostname'], 
									satori_user_id,
									event_data['satori_newpatname'],
									event_data)
								
								if satori_pat_temp[1] in (400, 409):
									print("Duplicate Pat Found or PAT Format invalid")
								else:
									print("\nreceived a new satori PAT: " + str(satori_pat_temp[0]))
									satori_pat = satori_pat_temp[0]

							else:
								print("reusing existing PAT: " + satori_pat['tokenName'])
								connection_url = event_data['tableau_url'] + "/workbooks/" + workbook_id + "/connections/" + connection_id

								updated_content = tableau_common.update_one_connection(
									connection_url, 
									tableau_headers, 
									satori_datastore['satoriHostname'], 		
									satori_pat['tokenName'],
									satori_pat['token'],
									event_data)

								print('\nconnection updated with this response: \n')
								print(updated_content)



def revert_user_connections(event_data):

	tableau_headers = tableau_common.build_tableau_header(event_data['tableau_token'])
	satori_headers = satori_common.satori_build_header(event_data['satori_token'])

	#first, verify the content owner and Satori Datastore both exist in Satori
	satori_user = satori_common.get_one_user(
			satori_headers, 
			event_data['satori_account_id'],
			event_data['satori_api_hostname'],
			event_data['owner'],
			event_data)

	satori_datastore = satori_common.get_one_datastore(
		satori_headers, 
		event_data['satori_api_hostname'],
		event_data['datastore_id']
		)


	if (satori_user is None) or (satori_datastore is None):
		print("no Satori user or Satori Datastore found, aborting")

	else:

		print("\n\nfound this satori user: " + str(satori_user) + '\n\nNow finding any Tableau content for this user')
		satori_user_id = satori_user['id']

		tableau_datasources = tableau_common.get_all_tableau_datasources(tableau_headers, event_data)
		tableau_workbooks = tableau_common.get_all_tableau_workbooks(tableau_headers, event_data)

		if (len(tableau_datasources) == 0) and (len(tableau_workbooks) == 0):
			print("no content found for this user, aborting without generating a Satori PAT")
		else:

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

						print('name: ' + datasource_name)
						print('owner: ' + datasource_owner)
						print('project: ' + datasource_project_name)
						print('datasource ID: ' + datasource_id)
						print('connection ID: ' + connection_id)
						print('hostname: ' + connection_serverAddress)
						print('')

						if (connection_serverAddress == satori_datastore['satoriHostname']):
							print('FOUND A HOSTNAME TO REVERT')

							connection_url = event_data['tableau_url'] + "/datasources/" + datasource_id + "/connections/" + connection_id


							updated_content = tableau_common.update_one_connection(
								connection_url, 
								tableau_headers, 
								satori_datastore['hostname'], 		
								event_data['newusername'],
								event_data['newpassword'],
								event_data)

							print('\nconnection reverted with this response: \n')
							print(updated_content)


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

						print('name: ' + workbook_name)
						print('owner: ' + workbook_owner)
						print('project: ' + workbook_project_name)
						print('workbook url: ' + workbook_url)
						print('datasource ID: ' + connection_datasource_id)
						print('connection ID: ' + connection_id)
						print('hostname: ' + connection_serverAddress)
						print('')

			
						if (connection_serverAddress == satori_datastore['satoriHostname']):
							print('FOUND A HOSTNAME TO REVERT')

						
							connection_url = event_data['tableau_url'] + "/workbooks/" + workbook_id + "/connections/" + connection_id

							updated_content = tableau_common.update_one_connection(
								connection_url, 
								tableau_headers, 
								satori_datastore['hostname'], 		
								event_data['newusername'],
								event_data['newpassword'],
								event_data)

							print('\nconnection reverted with this response: \n')
							print(updated_content)