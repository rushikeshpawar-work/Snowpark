from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import StringType, TimestampType
from datetime import datetime
from  snowpark_session import snowpark_session

session = snowpark_session()

@udf (name="timestamp_format",return_type=TimestampType(), input_types=[StringType()])
def timestamp_format(t):
    # Convert the timestamp string to a datetime object
    timestamp = datetime.strptime(t, '%a, %d %b %Y %H:%M:%S %Z')
    return timestamp


@udf (name="filename",return_type=StringType(), input_types=[StringType()])
def filename(x):
    l = x.split('/')
    return l[-1]
