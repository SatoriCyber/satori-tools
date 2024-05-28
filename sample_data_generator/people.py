import core as c
import helpers as h
import transactions
import social
import random

#if you append new columns, you need to ensure you have defined a handler for each of these in helpers.py
#we have provided a few as examples

#these are in addition to the core columns found in core.py
#their handlers are found in helpers.py
extra_columns = [ 
"job_type",
"account_type",
"phone_number",
"ssn",
"allergies",
"blood_type",
"last_ipaddress"
]

#the main function
def create_data(headers: bool, rows: int, buildtransactions: bool, *args) -> str:

	#our three csv output variables. note: these are just big strings
	#this was the purpose of the exercise for this entire solution :)

	people_data = ""
	transaction_data = ""
	social_data = ""

	people_data_json = ""
	transaction_data_json = ""


	#json array, so start with an open bracket
	people_data_json += c.openBracket + c.newline
	transaction_data_json += c.openBracket + c.newline

	# if asked for headers, first we print out the core headers, followed by the extra ones
	if headers:
		#core columns
		for idx, item in enumerate(c.coreColumns):
			people_data += c.quote + item + c.quote
			if idx+1 != len(c.coreColumns):
				people_data += c.comma
		#extra columns
		if len(extra_columns) > 0:
			people_data += c.comma
		for idx, item in enumerate(extra_columns):
			people_data +=c.quote + item + c.quote
			if idx+1 != len(extra_columns):
				people_data += c.comma
		people_data += c.newline

	# if asked for transaction and social data (related child records) we print out their headers first, if headers is True, 
	if buildtransactions & headers:
		for idx, item in enumerate(transactions.column_data):
			transaction_data += c.quote + item + c.quote
			if idx+1 != len(transactions.column_data):
				transaction_data += c.comma
		transaction_data += c.newline

		for idx, item in enumerate(social.column_data):
			social_data += c.quote + item + c.quote
			if idx+1 != len(social.column_data):
				social_data += c.comma
		social_data += c.newline

	# done with people.csv headers, now start the main iter
	rowcount = 0
	for newrow in range(rows):
		newrow = ""
		identity_bundle = c.handlerMap("identity_bundle")
		geolocation_bundle = c.handlerMap("geolocation_bundle")
		newid = c.handlerMap("customer_id")
		birthdate = str(c.handlerMap("birth_dt"))
		is_deleted = c.handlerMap("is_deleted")

		# 1 the ID
		newrow += c.quote + newid + c.quotecomma
		# 2 the identity info
		for item in range(len(identity_bundle)):
			newrow += c.quote + identity_bundle[item] + c.quotecomma
		# 3 the geo info
		for item in range(len(geolocation_bundle)):
			newrow += c.quote + geolocation_bundle[item] + c.quotecomma
		# 4 birthdate
		newrow += c.quote + birthdate + c.quotecomma
		# 5 isDeleted flag, to emulate salesforce or other systems that mark records as deleted
		newrow += str(is_deleted) + c.comma
		# 6 for any extra columns you have defined, call their handler in helpers.py
		for idx, item in enumerate(extra_columns):					
			newrow += c.quote + h.extraHandlerMap(item) + c.quote
			newrow += c.comma if idx+1 != len(extra_columns) else ""
		#7 add a newline if not last line
		if rowcount != rows:														
			newrow += c.newline
		
		people_data+= newrow


		# NOW THE JSON OUTPUT

		json_newrow = ""

		json_newrow += c.openCurly + c.newline
		
		# 1 the ID and identity bundle
		json_newrow += c.quote + "customer_id" + c.quote + c.colon	+ c.quote + newid + c.quote	+ c.comma + c.newline
		json_newrow += c.quote + "gender" + c.quote + c.colon	+ c.quote + identity_bundle[0] + c.quote + c.comma + c.newline
		json_newrow += c.quote + "name_prefix" + c.quote + c.colon	+ c.quote + identity_bundle[1] + c.quote + c.comma + c.newline
		json_newrow += c.quote + "name_first" + c.quote + c.colon	+ c.quote + identity_bundle[2] + c.quote + c.comma + c.newline
		json_newrow += c.quote + "name_last" + c.quote + c.colon	+ c.quote + identity_bundle[3] + c.quote + c.comma + c.newline
		json_newrow += c.quote + "email" + c.quote + c.colon	+ c.quote + identity_bundle[4] + c.quote + c.comma + c.newline
		json_newrow += c.quote + "employment" + c.quote + c.colon	+ c.quote + identity_bundle[5] + c.quote + c.comma + c.newline

		#geo info
		json_newrow += c.quote + "address_info" + c.quote + c.colon + c.openCurly + c.newline
		json_newrow += c.tab + c.quote + "address" + c.quote + c.colon	+ c.quote + geolocation_bundle[0] + c.quote + c.comma + c.newline
		json_newrow += c.tab + c.quote + "city" + c.quote + c.colon	+ c.quote + geolocation_bundle[1] + c.quote + c.comma + c.newline
		json_newrow += c.tab + c.quote + "county" + c.quote + c.colon	+ c.quote + geolocation_bundle[2] + c.quote + c.comma + c.newline
		json_newrow += c.tab + c.quote + "state" + c.quote + c.colon	+ c.quote + geolocation_bundle[3] + c.quote + c.comma + c.newline
		json_newrow += c.tab + c.quote + "postal_code" + c.quote + c.colon	+ c.quote + geolocation_bundle[4] + c.quote + c.newline
		json_newrow += c.tab + c.closeCurly + c.comma + c.newline

		json_newrow += c.quote + "birth_dt" + c.quote + c.colon	+ c.quote + birthdate + c.quote + c.comma + c.newline

		#extra info
		json_newrow += c.quote + "extra_info" + c.quote + c.colon + c.openCurly + c.newline		
		for idx, item in enumerate(extra_columns):
			if idx != len(extra_columns)-1:
				json_newrow += c.tab + c.quote + extra_columns[idx] + c.quote + c.colon + c.quote + h.extraHandlerMap(item) + c.quote + c.comma + c.newline
			else:
				json_newrow += c.tab + c.quote + extra_columns[idx] + c.quote + c.colon + c.quote + h.extraHandlerMap(item) + c.quote + c.newline
		json_newrow += c.tab + c.closeCurly + c.comma + c.newline

		json_newrow += c.quote + "is_deleted" + c.quote + c.colon + str(is_deleted) + c.newline

		#final row? ...For comma or no comma
		if rowcount == rows-1:
			json_newrow += c.closeCurly + c.newline
		else:
			json_newrow += c.closeCurly + c.comma + c.newline


		people_data_json+= json_newrow


		if buildtransactions:

			maxrows = int(args[0])
			random_num_transactions = random.randint(1,maxrows)

			#generate transaction data
			transaction_data += transactions.generate_transactions(newid, random_num_transactions)

			#generate JSON transaction data
			transaction_data_json += transactions.generate_transactions_json(newid, random_num_transactions, rowcount, rows)

			#generate social data
			#in the next line, identity_bundle[4] is the email address, we are sending it into the social data
			social_data += social.generate_social(newid, maxrows, identity_bundle[4])

		rowcount+=1

	#json array needs a closing bracket
	people_data_json += c.closeBracket
	transaction_data_json += c.closeBracket

	return people_data, people_data_json, transaction_data, transaction_data_json, social_data
