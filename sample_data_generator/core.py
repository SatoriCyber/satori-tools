import random
import datetime
import time
import string
import os

import inputs as i

# this file does a few things
# 1. creates an identity bundle, 
# 2. creates a geolocation bundle, 
# 3. loads a bunch of input/seed data into memory
# 4. loads some handlers into a handler map/memory

#let's define a 'recent' timestamp range going back a max of 1800 days
trans_start_date = datetime.datetime.now() - datetime.timedelta(days=1800)
duration = datetime.datetime.now() - trans_start_date
max_seconds = int(duration.total_seconds())

#this is for raw creation of csv files
quote				= "\""
quotecomma			= "\","
comma				= ","
newline				= "\n"
openBracket			= "["
closeBracket		= "]"
openCurly			= "{"
closeCurly			= "}"
colon				= ":"
tab					= "\t"

#these are our required core columns for the people.csv file
coreColumns = [
'customer_id',
'gender',
'name_prefix',
'name_first',
'name_last',
'email',
'employment',
'address',
'city',
'county',
'state',
'postal_code',
'birth_dt',
'is_deleted'
]


#the following handlers deal with all of the above columns
handlers = {
"is_deleted" : "isDeletedHandler",
"customer_id" : "nextId",
"identity_bundle" : "coreIdentityBundle",
"geolocation_bundle" : "coreGeolocationBundle",
"birth_dt" : "birthDateHandler"
}

#random generator that takes input
def randomlySelected(chanceof, range):
	# e.g. if 6 out of 13 chance odds
	if random.randrange(range) < chanceof:
		return True
	else:
		return False

#static and relative dates for birthdays
earliest_possible_birthday = datetime.date.today() - datetime.timedelta(days=27300)

#birthday_end_date = datetime.date.today() - datetime.timedelta(days=5096)
#time_between_dates = birthday_end_date - birthday_start_date
#birthday_days_between_dates = time_between_dates.days

# generates a 16 char random key
def nextId(size=16, chars=string.ascii_lowercase + string.digits):
	uniqueid = ''.join(random.choice(chars) for _ in range(size))
	return uniqueid

#  generates an identity bundle, defined as: "Gender, Prefix, First Name, Last Name, Email, Employer"
def coreIdentityBundle():

	gender, prefix, firstname, lastname, email, employer = [""]*6
	identity = []
	
	gender = "F" if randomlySelected(6, 11) else "M"
	identity.append(gender)

	prefix = random.choice(i.df_prefix_female) if gender == "F" else random.choice(i.df_prefix_male)
	identity.append(prefix)

	firstname = random.choice(i.df_firstnames_female) if gender == "F" else random.choice(i.df_firstnames_male)
	identity.append(firstname)

	lastname = random.choice(i.df_lastnames)
	identity.append(lastname)
	
	employment = random.choice(i.df_companies)
	temptld = random.choice(i.df_tld)

	# out of 100, what are we going to do for a variety of emails?
	dice = random.randint(1,100)
	if dice >= 75:
		# email gen random word plus first name
		email = ''.join(random.choice(i.df_randowords)) + "_" + firstname.lower() + "@" + ''.join(e for e in employment if e.isalnum()).lower() + '.' + temptld
	elif dice >= 50:
		# FIRST AND LAST NAME
		email = firstname.lower() + lastname.lower() + "@" + ''.join(e for e in employment if e.isalnum()).lower() + '.' + temptld
	elif dice >= 25:
		#FIRST INITIAL, LAST NAME, AND A INTEGER
		email = firstname[0].lower() + lastname.lower() + str(random.randint(100,999)-1) + "@" + ''.join(e for e in employment if e.isalnum()).lower() + '.' + temptld
	else:
		# FIRST INITIAL AND LAST NAME
		email = firstname[0].lower() + lastname.lower() + "@" + ''.join(e for e in employment if e.isalnum()).lower() + '.' + temptld

	identity.append(email)
	identity.append(employment)
	
	return identity

#  generates an geolocation bundle, defined as: "address, city, state, postalcode"
def coreGeolocationBundle():
	geolocation = []
	geolocation.append( str(random.randint(100,9999)) + " " + random.choice(i.df_streetnames))
	citystatecombo = random.choice(i.df_city_county_state).split(',')
	geolocation.append(citystatecombo[0])
	geolocation.append(citystatecombo[1])
	geolocation.append(citystatecombo[2])
	geolocation.append(random.choice(i.df_postalcodes))
	return geolocation

# picks a random birthday
def birthDateHandler():
	random_birthdate = earliest_possible_birthday + datetime.timedelta(days=random.randint(5096,27300))		
	return random_birthdate

def isDeletedHandler():
	# 15% of our records will be marked as isDeleted
	deleteflag = 1 if randomlySelected(2, 15) else 0
	return deleteflag	


def handlerMap(type):
	return globals()[handlers.get(type)]()


#print(handlers.get("is_deleted"))

#testisDeleted = globals()[handlers.get("deleted2")]()
#print(testisDeleted)

