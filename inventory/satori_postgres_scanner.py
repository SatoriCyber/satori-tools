import psycopg2
import time

# select one or more postgres schemas using the following format
TARGET_SCHEMAS = ('public', 'second_schema', 'third_etc')

# how many rows in each select statement
ROWS_TO_PULL = '1200'

# connection info
satori_host = 'YOUR_SATORI_ENDPOINT.us-east-1.a.p0.satoricyber.net'
satori_port = 12345
database_name = 'YOUR_DB_NAME'
satori_username = 'SATORI_USER'
satori_password = 'SATORI_PASSWORD'


###########################
# no change below this line

satori_audit_conn = psycopg2.connect(
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
    time.sleep(2) # try to prevent security isues from kicking in, e.g. firewalls, cloudfront et al