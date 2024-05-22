# pip install redshift_connector
import redshift_connector
import time

# THIS SCRIPT will run queries on all tables in the selected schemas
# This will in turn trigger Satori's Data Inventory features

# select one or more redshift schemas using this format
TARGET_SCHEMAS = ('public', 'another_schema', 'a_third_schema')

# how many rows in each select statement
ROWS_TO_PULL = '1200'

# connection info
satori_host = 'YOUR_SATORI_HOST_INFO.a.p0.satoricyber.net'
satori_port = 12340
database_name = 'YOUR_DB_NAME'
satori_username = 'SATORI_USER'
satori_password = 'SATORI_PASSWORD'


###################################
# no changes needed below this line

satori_audit_conn = redshift_connector.connect(
    host=satori_host,
    database=database_name,
    port=satori_port,
    user=satori_username,
    password=satori_password
)

scanner = satori_audit_conn.cursor()

scanner_sql = "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA in {};".format(TARGET_SCHEMAS)

scanner.execute(scanner_sql)
scanner_rows = scanner.fetchall()
tables = []

for record in scanner_rows:
    table_name = record[2]
    db_name = record[0]
    schema_name = record[1]
    fully_qualified_location = "\"{}\".\"{}\".\"{}\"".format(db_name.replace("\"", "\"\""), schema_name.replace("\"", "\"\""), table_name.replace("\"", "\"\""))
    tables.append(fully_qualified_location)

for table in tables:
    table_str_sql = "SELECT * FROM {} LIMIT {};".format(table, ROWS_TO_PULL)
    print("running: " + table_str_sql)
    satori_audit_conn.cursor().execute(table_str_sql)
    time.sleep(2) # let's try to prevent security isues from kicking in, e.g. firewalls, cloudfront et al