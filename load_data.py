from snowpark_session import snowpark_session
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from snowflake.snowpark.functions import to_date,col
from get_file_name import get_files_names
session = snowpark_session()

# Define the schema for the CSV data
csv_schema = StructType([
    StructField("Row ID", IntegerType(), True),
    StructField("Order ID", StringType(), True),
    StructField("Order Date", StringType(), True),
    StructField("Ship Date", StringType(), True),
    StructField("Ship Mode", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Postal Code", StringType(), True),
    StructField("Market", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub-Category", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("Sales", DoubleType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Profit", DoubleType(), True),
    StructField("Shipping Cost", DoubleType(), True),
    StructField("Order Priority", StringType(), True)
])

filename_list = get_files_names()

if bool(filename_list):
  empty_df = session.createDataFrame([], schema=csv_schema)

  for filename in filename_list:
    #option("INFER_SCHEMA", True)
    df = session.read.options({"field_delimiter": ","})\
      .option("encoding" , 'iso-8859-1' ).schema(csv_schema)\
      .option("SKIP_HEADER",1).csv(f"@my_azure_stage/{filename}")
    
    empty_df.union_all(df)


  data_df = empty_df.withColumn("Order Date", to_date(df["Order Date"], "dd-MM-yyyy"))\
            .withColumn("Ship Date", to_date(df["Ship Date"], "dd-MM-yyyy"))


  data_df.printSchema()

  # create_table_query = '''CREATE TABLE IF NOT EXISTS RAW_SUPERSTORE_DATA(
  #   "Row ID" NUMBER(38,0),
  #   "Order ID" VARCHAR(16777216),
  #   "Ship Mode" VARCHAR(16777216),
  #   "Customer ID" VARCHAR(16777216),
  #   "Customer Name" VARCHAR(16777216),
  #   "SEGMENT" VARCHAR(16777216),
  #   "CITY" VARCHAR(16777216),
  #   "STATE" VARCHAR(16777216),
  #   "COUNTRY" VARCHAR(16777216),
  #   "Postal Code" VARCHAR(16777216),
  #   "MARKET" VARCHAR(16777216),
  #   "REGION" VARCHAR(16777216),
  #   "Product ID" VARCHAR(16777216),
  #   "CATEGORY" VARCHAR(16777216),
  #   "Sub-Category" VARCHAR(16777216),
  #   "Product Name" VARCHAR(16777216),
  #   "SALES" DOUBLE PRECISION,
  #   "QUANTITY" NUMBER(38,0),
  #   "DISCOUNT" DOUBLE PRECISION,
  #   "PROFIT" DOUBLE PRECISION,
  #   "Shipping Cost" DOUBLE PRECISION,
  #   "Order Priority" VARCHAR(16777216),
  #   "Order Date" DATE,
  #   "Ship Date" DATE );'''

  data_df.write.save_as_table("RAW_SUPERSTORE_DATA", mode="APPEND")

else:
  print('No new file is inserted')