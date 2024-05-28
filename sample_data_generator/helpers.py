import string
import random
import os
import datetime

import core as c
import inputs as i

extrahandlers = {
"account_type" 			: "AccountHandler",
"phone_number" 			: "PhoneHandler",
"job_type" 				: "JobTypeHandler",
"cc_number" 			: "CCNumberHandler",
"ssn" 					: "SsnHandler",
"allergies" 			: "AllergyHandler",
"blood_type" 			: "BloodTypeHandler",
"last_ipaddress" 		: "IPAddressHandler",
"social_last_uri" 		: "SocialLastUriHandler",
"social_timestamp" 		: "SocialTimestampHandler",
"social_ip" 			: "SocialIPAddressHandler",
"social_sha256" 		: "SocialShaHandler",
"hashtags"				: "HashtagHandler",
"social_uuid4" 			: "SocialUuid4Handler"
}

def AccountHandler():
	return random.choice(i.df_account_types)

def PhoneHandler():
	return random.choice(i.df_phones)

def JobTypeHandler():
	return random.choice(i.df_jobs)

def SsnHandler():
	return str(random.randint(100,999)) + "-" + str(random.randint(1,99)) + "-" + str(random.randint(1000,9999))

def AllergyHandler():
	return random.choice(i.df_allergies)

def BloodTypeHandler():
	return random.choice(i.df_bloodtypes)

def IPAddressHandler():
	return random.choice(i.df_ip_addresses)

def SocialLastUriHandler():
	return random.choice(i.df_uris)

def SocialTimestampHandler():
	random_datetime = c.trans_start_date + datetime.timedelta(seconds=random.randint(1,c.max_seconds))	
	return str(random_datetime.replace(microsecond=0))

def SocialIPAddressHandler():
	return random.choice(i.df_ip_addresses)

def SocialShaHandler():
	return random.choice(i.df_sha256)

def SocialUuid4Handler():
	return random.choice(i.df_ip_addresses)

def HashtagHandler():
	if c.randomlySelected(11, 15):
		verb = random.choice(i.df_verbs)
		noun = random.choice(i.df_nouns)
		noun2 = random.choice(i.df_nouns)
		return "#" + noun + ", #" + verb + "_" + noun + ", #" + noun2 + "_" + noun
	else:
		verb = random.choice(i.df_verbs)
		noun = random.choice(i.df_nouns)
		return "#" + noun + ", #" + verb + "_" + noun



# our handler map
def extraHandlerMap(type):
	return globals()[extrahandlers.get(type)]()


