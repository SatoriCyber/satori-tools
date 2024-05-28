import sys
import requests
import json
import argparse
import os

import satori_report
import satori_update_single
import satori_update_multiple
import satori_common
import tableau_common


#event_data moves around all of this utility with all the necessary info to perform 
#the various functions e.g. report, update single, update multiple, etc
#change all of these vars to match your Satori and Tableau environments:

event_data = {
		"tableau_base_url": "prod-useast-a.online.tableau.com",
		"tableau_api_version": "3.14",
		"verify_ssl": False,
		"tableau_page_size": "1000",
		"satori_api_hostname": "app.satoricyber.com",
		#for governance reporting, you can hand in an array of search fragments, e.g. 'satoricyber.net'
		"dac_search": ["satoricyber.net", "us-east1.someotherhost.com", "thirdhost.somewhere.com"]
	}


#we define a main as well as a "__main__" for future AWS Lambda integration work
def main(event_data, context):      

	event_mode = event_data['mode']

	# env must be set or else fail
	event_data['satori_account_id'] = os.getenv('satori_account_id')
	event_data['satori_sa_id'] = os.getenv('satori_sa_id')
	event_data['satori_sa_key'] = os.getenv('satori_sa_key')
	event_data['tableau_patname'] = os.getenv('tableau_patname')
	event_data['tableau_patsecret'] = os.getenv('tableau_patsecret')
	
	# get a tableau session token or else fail
	tableau_auth = tableau_common.get_tableau_token(event_data)
	event_data['tableau_token'] = tableau_auth[0]
	event_data['site_id'] = tableau_auth[1]

	# get a satori session token or else fail	
	event_data['satori_token'] = satori_common.satori_auth(event_data)

	#generate the base tableau URL for all other tableau URLs
	event_data['tableau_url'] = "https://{}/api/{}/sites/{}".format(
		event_data['tableau_base_url'],
		event_data['tableau_api_version'],
		event_data['site_id'])

	#if searching for an email/owner, url encode the email because of plus signs 
	event_data['owner_urlencoded'] = event_data['owner'].replace('+','%2b')

	#what operation are we performing?
	if event_mode == 'reportall':
		event_data['filename'] = "SatoriGovernanceReport.csv"
		satori_report.analyze_connections(event_data)
		
	if event_mode == 'reportdatastore':
		event_data['filename'] = "SatoriGovernanceReport-" + event_data['datastore_id']  + ".csv"
		satori_report.analyze_connections(event_data)

	if event_mode == 'reportowner':
		event_data['filename'] = "SatoriGovernanceReport-" + event_data['owner']  + ".csv"
		satori_report.analyze_connections(event_data)

	if event_mode == 'update_with_pat':
		satori_update_single.govern_single_connection(event_data)

	if event_mode == 'update_multiple_with_pat':
		satori_update_multiple.govern_user_connections(event_data)

	if event_mode == 'revert_multiple':
		satori_update_multiple.revert_user_connections(event_data)

	if event_mode == 'manual':
		satori_update_single.manual_single_connection(event_data)


if __name__ == "__main__":
	
	parser = argparse.ArgumentParser()
	parser.add_argument('-v', '--verbose', action='store_true')

	parser.add_argument('command', type=str, nargs='?', default='report',
					help='The type of command to run, choices are list, govern and ungovern')
	parser.add_argument('-content_id', type=str, nargs='?', default='',
					help='The ID of the Tableau content')
	parser.add_argument('-connection_id', type=str, nargs='?', default='',
					help='The ID of the Tableau connection')
	parser.add_argument('-datastore_id', type=str, nargs='?', default='',
					help='The ID of the Satori Datastore')
	parser.add_argument('-satori_newpatname', type=str, nargs='?', default='',
					help='The desired new name for a new Satori Personal Access Token')
	parser.add_argument('-owner', type=str, nargs='?', default='',
					help='The owner email for the tableau content')
	parser.add_argument('-oldhostname', type=str, nargs='?', default='',
					help='The old hostname to replace')
	parser.add_argument('-newhostname', type=str, nargs='?', default='',
					help='The new hostname for the connection')
	parser.add_argument('-newusername', type=str, nargs='?', default='',
					help='The new username for the connection to update')
	parser.add_argument('-newpassword', type=str, nargs='?', default='',
					help='The new password for the connection to update')

	args = parser.parse_args()

	event_data['mode'] 				= args.command if args.command else 'reportall'
	event_data['content_id'] 		= args.content_id if args.content_id else ''
	event_data['connection_id'] 	= args.connection_id if args.connection_id else ''
	event_data['datastore_id'] 		= args.datastore_id if args.datastore_id else ''
	event_data['satori_newpatname'] = args.satori_newpatname if args.satori_newpatname else ''
	event_data['owner'] 			= args.owner if args.owner else ''
	event_data['oldhostname'] 		= args.oldhostname if args.oldhostname else ''
	event_data['newhostname'] 		= args.newhostname if args.newhostname else ''
	event_data['newusername'] 		= args.newusername if args.newusername else ''
	event_data['newpassword'] 		= args.newpassword if args.newpassword else ''

	main(
		event_data, 
		context="comes from aws event or from local console"
		)
