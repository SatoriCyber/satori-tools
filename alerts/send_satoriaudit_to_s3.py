import json
import requests
import datetime
import io
import boto3

# simple example showing how to pull one day of Satori audit data, 
# create a csv object, and store that as a file in S3 where the date is the S2 path


# Satori Authentication
# see https://app.satoricyber.com/docs/api for auth info

satori_serviceaccount_id  = "MUST_FILL_IN"
satori_serviceaccount_key = "MUST_FILL_IN"
satori_account_id         = "MUST_FILL_IN"
satori_host               = "app.satoricyber.com"

#S3 Access Creds and bucket name - recommend implementing AWS secrets manager or similar
access_key = "YOUR_AWS_ACCESS_KEY"
secret = "YOUR_AWS_ACCESS_SECRET"
#S3 Bucket Name
your_bucket_name = "YOUR_S3_BUCKET"

# should we include admin queries that Satori captures?
admin_queries = "true"

# remainder of this example should run as-is for testing purposes

# Once a day, let's pull all of the Satori audit data and then push it to an S3 location
# get our start and end unix timestampe for 'yesterday' and also generate an S3 bucket path
# note: this script runs in local time zones, you may want to adjust for UTC

yesterday_start = datetime.date.today() - datetime.timedelta(1)
unix_time_start = yesterday_start.strftime("%s") + "000"
unix_time_end = str(int(yesterday_start.strftime("%s")) + (86400)) + "000"

#generate a S3 bucket path based upon our timeframes, e.g. "2023/05/2023.05.15.csv"
bucket_path = yesterday_start.strftime("%Y/%m/") + yesterday_start.strftime("%Y.%m.%d.csv")
#print(bucket_path)

# Retriever Function, Satori Audit Data via Rest API
def getAuditLogs():

    # Authenticate to Satori for a bearer token
    auth_headers = {'content-type': 'application/json','accept': 'application/json'}
    auth_url = "https://{}/api/authentication/token".format(satori_host)
    auth_body = json.dumps(
        {
            "serviceAccountId": satori_serviceaccount_id,
            "serviceAccountKey": satori_serviceaccount_key
        })
    
    try:
        r = requests.post(auth_url, headers=auth_headers, data=auth_body)
        response = r.json()
        satori_token = response["token"]
    except Exception as err:
        print("Bearer Token Failure: :", err)
        print("Exception TYPE:", type(err))
    else:
    
        # build request to rest API for audit entries, aka "data flows"
        # for more info and additional parameters you can use, see here:
        # https://app.satoricyber.com/docs/api#get-/api/data-flow/-accountId-/export
        
        admin_param = "includeAdministrateQueriesFilter=" + admin_queries
        payload = {}
        headers = {'Authorization': 'Bearer {}'.format(satori_token),}
        auditurl = "https://{}/api/data-flow/{}/export?from={}&to={}&{}".format(
                                                                             satori_host,
                                                                             satori_account_id,
                                                                             unix_time_start,
                                                                             unix_time_end,
                                                                             admin_param)
        try:
            response = requests.get(auditurl, headers=headers, data=payload)
            response.raise_for_status()
        except requests.exceptions.RequestException as err:
            print("Retrieval of audit data failed: :", err)
            print("Exception TYPE:", type(err))
        else:
            return response.text

# MAIN WORK FOLLOWS:

# 1. get our audit data, 
# 2. encode the data, and 
# 3. send the data to s3
# This all avoids a file write on the local system

audit_payload = getAuditLogs()

session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret)
s3 = session.resource("s3")

buff = io.BytesIO()
buff.write(audit_payload.encode())

try:
    s3.Object(your_bucket_name, bucket_path).put(Body=buff.getvalue())
    print("successfully uploaded csv to " + bucket_path)
except ClientError as e:
    logging.error(e)