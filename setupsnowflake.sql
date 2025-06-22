
USE ROLE ACCOUNTADMIN;
 
USE WAREHOUSE COMPUTE_WH;
USE DATABASE TEST;

-- Creating an external stage in Snowflake on Azure location
USE SCHEMA myschema;
CREATE STAGE my_azure_stage 
URL = 'azure://<container url>' 
CREDENTIALS = (AZURE_SAS_TOKEN = '<sas token>');

