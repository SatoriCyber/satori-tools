# This example deliberately excludes error control or try/catch blocks
# so that it can be short and legible. We recommend adding these as well as
# any other custom logic you may want.

# Goal: create an array of runnable queries by iterating through
# all tables in allowed schemas. The Satori Data Inventory will become populated.

# This process may not introspect on all nested field types e.g. json or other blob types.

import mysql.connector
import time

#an inclusion array of mysql schemas to scan
allowed_schemas = ['demomysqldb', 'someotherdb', 'a_third_db']

#each query will have a record limit
limit_per_table = 5000

#to prevent cloudflare or other cloud protection from kicking in,
#specify a sleep between each query run, in seconds
sleep_between_queries = 1

#edit the satori connection
satori_connection = mysql.connector.connect(
  
    #you can use a Satori user/pass here if you have enabled Satori Auth
    user='satori_auth_user',
    password='CHANGE',
    
    #the Satori Hostname, not the original MySQL hostname
    host='your-mysql-satori-db.us-east-1.a.p0.satoricyber.net',
    
    #the Satori port override, this can be found in the Satori Datastore config details
    port='12341'
    )

######################################################
#no changes below this line for example purposes
######################################################

def get_schemas():
    #GET FULL LIST OF SCHEMA.TABLES
    get_schema_sql = "select name from information_schema.INNODB_TABLES;"
    schema_cursor = satori_connection.cursor(buffered=True)    
    schema_cursor.execute(get_schema_sql)
    get_schema_sql_result = schema_cursor.fetchall()
    return get_schema_sql_result

def get_tables():
    #IF SCHEMA.TABLE IS IN ALLOWED LIST, CREATE ARRAY OF RUNNABLE QUERIES
    runnable_queries = []            
    for schematable in get_schemas():
        splitter = str(schematable).find('/')
        schema = str(schematable)[2:splitter]
        table = str(schematable)[splitter+1:len(str(schematable))-3]
        select_statement = "select * from " + schema + "." + table + " limit " + str(limit_per_table)
        if schema in allowed_schemas:
            runnable_queries.append(select_statement)
    return runnable_queries

def run_query(query):
    #RUN ONE OF THE RUNNABLE QUERIES
    run_query_cursor = satori_connection.cursor(buffered=True)
    run_query_cursor.execute(query)
    run_query_cursor.fetchall()
    run_query_cursor.close()
    time.sleep(sleep_between_queries)
    return "ran: " + query

# MAIN WORK
for satori_query in get_tables():
    output = run_query(str(satori_query))
    print(output)