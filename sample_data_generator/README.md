## simpledatagen - lightweight data generator for various uses

##### This app builds five files - people.csv, people.json, transactions.csv, transactions.json and social.csv - with a customer primary key between them. The premise is that you would then load this data into your favorite database or data science tool.

There are text inputs for various random data generation, in the './inputs' directory.

We've eliminated all possible 3rd party modules or lookups - 100% portable - these examples run with only:
- import random
- import datetime
- import time
- import string
- import os

as well as custom imports from this same directory (see 'inputs' directory)

##### how to run

```python gendata.py```

Generates five files:
- people.csv
- transactions.csv
- social.csv
- people.json
- transactions.json

Generates 1000 customers and each customer gets 1-10 transactions with a relational key, also 1-10 social media transactions, and outputs three CSV files to this same directory. We also output two json files

Change the args in gendata.py as you see fit:

```
# args: (csv headers?, how many rows?, create transactions?, max transactions per person?)
args = (True, 1000, True, 10)
```

##### people.json and transactions.json

Experimental: we are also generating two json files for use with MongoDB or similar. Once generated, you can run something like:
```
db.YOUR_COLLECTION.insertMany()
```

Note: this part of the code is hardwired for the fields that ship with this repo. If you deviate or add fields, you'll need to tinker with people.createJsonData() accordingly.

##### flaskserver.py

- Using *Flask* (you will need to _pip install flask_ in your python environment), we create two simple endpoints "/static" and "/dynamic" - which will both autogenerate 1,000 lines of fake people data by calling gendata. 
- For the static endpoint, the data is generated once per lifetime of the flask run. 
- For the dynamic endpoint, each browser refresh to the endpoint will regen the data.

from this directory, run:

```
export FLASK_APP=flaskserver.py
flask run
```

and then browse to either of:

http://127.0.0.1:5000/static
http://127.0.0.1:5000/dynamic

