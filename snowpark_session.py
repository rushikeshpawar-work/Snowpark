import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark import Session


def snowpark_session():
    connection_parameters = {
    "account": "<snoflake account>",
    "user": "<user name>",
    "password": "<password>",
    "role": "<user role>",
    "warehouse": "<warehouse name>",
    "database":"<database name>",
    "schema":"<schema>"}

    session = Session.builder.configs(connection_parameters).create()

    return session

# tableName = 'myschema.student'
# dataframe = session.table(tableName).filter(col("name") == 'raj')
# ct = dataframe.count()
# df = session.create_dataframe([ct],schema = ['count'])

# df.show()

