import snowflake.connector
from snowflake.connector.errors import DatabaseError

# BEGIN PARAMETERS, CHANGE THESE AS NEEDED:

# should we actually run the select statements or just print them to the console?
# if dryrun is set to False, we will actually run select statements on all the tables and views
# note: dryrun only applies to the select statements, this script will still run queries to find database and schema names
DRYRUN = True 

# which snowflake databases should we ignore?
IGNORED_DATABASES = ['SNOWFLAKE_SAMPLE_DATA', 'SNOWFLAKE']

# which snowflake schemas should we ignore?
IGNORED_SCHEMAS = ['INFORMATION_SCHEMA']

# the snowflake account fragment, i.e. the part that comes before snowflakecomputing.com
SNOWFLAKE_ACCOUNT = "abc12345"

# which snowflake warehouse for computing
SNOWFLAKE_WAREHOUSE = "PLAYGROUND"

# a valid snowflake SSO user, this script will pop open a browser window for SSO auth
SNOWFLAKE_USER = "your_sso_user@yourorg.com"

# the snowflake role of the specified user
ROLE = 'ACCOUNTADMIN'

# the Satori endpoint/hostname
SATORI_HOSTNAME = "FIND THIS IN THE SATORI APP for your Snowflake Datastore"

# how many rows to pull for each select statement
ROWS_TO_PULL = 500

# END OF PARAMETERS, BEGIN WORK:

# note: as written, this script requires Snowflake SSO
# Change the connection parameter to user/pass by
# uncommenting the password line, and commenting the authenticator line


con = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    #password=SNOWFLAKE_PASSWORD,
    authenticator='externalbrowser',
    host=SATORI_HOSTNAME,
    role=ROLE
)

databases_query = "show databases"
limiter = " limit " + str(ROWS_TO_PULL)
preamble = "select * from "
 
for databases in con.cursor().execute(databases_query):
    database = databases[1]
    if database not in IGNORED_DATABASES:
        print("WORKING WITH DATABASE: " + database)
        schemas_query = "show schemas in database " + database
        for schemas in con.cursor().execute(schemas_query):
            database_schema = database + '.' + schemas[1]
            schema = schemas[1]
            if schema not in IGNORED_SCHEMAS:
                print("WORKING WITH SCHEMA: " + schema)
                
                #FIRST: TABLES
                tables_query = "show tables in schema " + database_schema
                
                for tables in con.cursor().execute(tables_query):
                    table = database_schema + '.' + tables[1]
                    table_query = preamble + table + limiter
                    if DRYRUN == False:
                        try:
                            print("running query: " + table_query)
                            result = con.cursor().execute(table_query)
                        except DatabaseError as e:
                            print(e)
                    else:
                        print(table_query)
                        
                #SECOND: VIEWS
                views_query = "show views in schema " + database_schema
                
                for views in con.cursor().execute(views_query):
                    view = database_schema + '.' + views[1]
                    view_query = preamble + view + limiter
                    if DRYRUN == False:
                        try:
                            print("running query: " + view_query)
                            result = con.cursor().execute(view_query)
                        except DatabaseError as e:
                            print(e)
                    else:
                        print(view_query)