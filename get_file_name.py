import json
import re
from  snowpark_session import snowpark_session
from snowflake.snowpark.functions import col, udf, call_udf, to_timestamp, lit,unix_timestamp, array_max, array_agg
from datetime import datetime
from snowflake.snowpark.types import StringType, TimestampType

session = snowpark_session()

#--- UDF------------------------------------------------------------------------------

@udf (name="timestamp_format",return_type=TimestampType(), input_types=[StringType()])
def timestamp_format(t):
    # Convert the timestamp string to a datetime object
    timestamp = datetime.strptime(t, '%a, %d %b %Y %H:%M:%S %Z')
    return timestamp

@udf (name="filename",return_type=StringType(), input_types=[StringType()])
def filename(x):
    l = x.split('/')
    return l[-1]

#----- Fuctions-----

def extract_timestamp(element):
    t_stamp = element.TIMESTAMP
    return datetime.strptime(str(t_stamp), "%Y-%m-%d %H:%M:%S")


def extract_filename(name):
    return name.FILENAME

#---------------------------------------------------------------------------

def get_files_names():

    with open("timestamp.json",'r') as f:
        data = json.load(f)

    previous_timestamp = datetime.strptime(data['timestamp'], "%Y-%m-%d %H:%M:%S")

    query = "list @my_azure_stage"

    metadata = session.sql(query).collect()

    metadata_df = session.create_dataframe(metadata)

    metadata_df.sort(col("LAST_MODIFIED"), ascending=False).show()

    metadata_df = metadata_df.with_column('Timestamp',  call_udf('timestamp_format',col("LAST_MODIFIED")))

    metadata_df.show()

    timestamp_list = metadata_df.select('Timestamp').collect()

    t_list = [extract_timestamp(row) for row in timestamp_list]

    # latest_file = metadata_df.filter(metadata_df["timestamp"] > max(t_list)).collect()
    latest_file = metadata_df.filter(metadata_df["timestamp"] > previous_timestamp).collect()

    f_name_list = []

    if bool(latest_file):
        latest_df = session.create_dataframe(latest_file)

        latest_df.show()

        final_df = latest_df.with_column('Filename',  call_udf('filename',col("NAME")))

        f_name_list = final_df.select('Filename').collect()

    filename_list = [extract_filename(name) for name in f_name_list]

    data['timestamp'] = str(max(t_list))

    with open("timestamp.json",'w') as f:
        # data = json.load(f)
        json.dump(data, f)
    f.close()

    if bool(filename_list):
        return filename_list
    else:
        return []


print(get_files_names())