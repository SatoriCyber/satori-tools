# Please read the full documentation for Satori access to AWS S3 here:
# https://satoricyber.com/docs/datastores/s3/
# this example assumes you have already created a Satori Datastore for your AWS S3 access

# pip install boto3 and pandas for this example to work

import os
import boto3
import json
import pandas as pd


# build a boto3 client

satori_allowed = boto3.client(
    's3',
    # SATORI HOSTNAME which overrides S3 URI:
    endpoint_url = 'https://your-satori-hostname.region.p1.satoricyber.net',
    # SATORI CREDENTIAL
    # You do not use an amazon account or access ID
    # instead, you use your Satori generated username and password!
    aws_access_key_id='YOUR_SATORI_USER_ID', 
    #SATORI TEMP PASS
    aws_secret_access_key='YOUR_SATORI_PASS', 
    #S3 REGION, required for boto
    region_name='us-east-1'
    )

# For the following simple examples, make sure your Satori S3 connection has been added to a Satori Dataset
# and that you have permission to access the S3 locations in this Dataset.

# Let's get some file names
# change "YOUR-S3-BUCKET" and the prefix/folder "allowed" to valid locations

satori_filelist = satori_allowed.list_objects_v2(
    Bucket='YOUR-S3-BUCKET',
    Prefix ='allowed',
    MaxKeys=1000 )

files = satori_filelist.get("Contents")
for file in files:
    print(f"file_name: {file['Key']}, size: {file['Size']}")
    
# Let's get some csv data
# change "YOUR-S3-BUCKET" and "some-folder/some-file" to a valid location

data = satori_allowed.get_object(Bucket='YOUR-S3-BUCKET', Key='some-folder/some-file.csv')
data_df = pd.read_csv(data.get("Body"))
data_df.head()