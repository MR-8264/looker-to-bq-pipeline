import os
import time
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import looker_sdk

client = bigquery.Client()
sdk = looker_sdk.init40()

# Global Variables
looks_to_transfer = ("Look_Name_01", "Look_Name_02")

def main(request):
    try:
        for i in looks_to_transfer:
            looker_name = i
            bq_table_name = get_table_name(looker_name)
            retrieved_look_id = get_look_id(looker_name)  # Get look id from Looker using look name
            data = get_data_from_looker(retrieved_look_id)  # Get the data from Looker using the query ID
            write_to_file(data)  # Pass the data to write_to_file
            load_to_bq()  # Create and load data into the staging table
            deduplicate_data(bq_table_name)  # De-duplicate and merge data into the main table
    finally:
        clean_up_staging_table()  # Ensure staging tables are deleted
    return "Successfully loaded and de-duplicated data from Looker to BigQuery"

# Get look ID from look name with Looker's 'Search Looks' API call
def get_look_id(look_name):
    id = sdk.search_looks(
        title=look_name,
        fields="id")
    return id[0].id

# Run a query with look ID and return response as a csv using Looker's 'Run Look' API call
def get_data_from_looker(look_id):
    response = sdk.run_look(
        look_id=look_id,
        result_format="csv")
    return response

# Transform data to BigQuery format and fix column names
def write_to_file(data):
    cnt = 0
    for i in data: 
        if i == "\n":
            break
        else: 
            cnt += 1
    header = data[:cnt]
    header_to_write = header.replace(" ", "_")
    data_to_write = data[cnt:]
    
    # Write header and data to temporary disk
    with open('/tmp/table.csv', "w") as csv:
        csv.write(header_to_write)  # Write header to file
        csv.write(data_to_write)    # Write data to file

# Load data to BigQuery with BigQuery API call
def load_to_bq():
    staging_table_id = "junction-health-data.weinfuse.staging"
    
    # Load CSV data into the staging table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,  # Automatically detect the schema
        write_disposition="WRITE_TRUNCATE",  # Overwrite the staging table if it exists
    )
    with open("/tmp/table.csv", "rb") as source_file:
        job = client.load_table_from_file(source_file, staging_table_id, job_config=job_config)
    job.result()  # Wait for the load job to complete.

# Transform table name to BigQuery format
def get_table_name(looker_name):
    return looker_name.replace(" ", "_")

# Prevent duplicate data from being written to BQ table if it already exitsts
def deduplicate_data(bq_table_name):
    main_table_id = f"junction-health-data.weinfuse.{bq_table_name}"
    staging_table_id = f"junction-health-data.weinfuse.staging"
    
    # Check if main table exists
    try:
        client.get_table(main_table_id)  # Make an API request to check if the main table exists.
        
        # Ensure the 'Unique_ID' column exists in both the main and staging tables
        main_table = client.get_table(main_table_id)
        staging_table = client.get_table(staging_table_id)

        # Check if Unique_ID exists in both tables
        main_columns = [field.name for field in main_table.schema]
        staging_columns = [field.name for field in staging_table.schema]

        if 'Unique_ID' not in main_columns or 'Unique_ID' not in staging_columns:
            raise ValueError("Unique_ID column is missing in either the main or staging table.")
        
        # De-duplicate and merge data
        query = f"""
        MERGE {main_table_id} AS main
        USING {staging_table_id} AS staging
        ON main.Unique_ID = staging.Unique_ID  -- Make sure Unique_ID exists in both tables
        WHEN MATCHED THEN
            UPDATE SET main.Col1 = staging.Col1, main.Col2 = staging.Col2  -- Add more columns as needed
        WHEN NOT MATCHED THEN
            INSERT ROW
        """
        query_job = client.query(query)  # Run the query
        query_job.result()  # Wait for the job to complete.
    
    except NotFound:
        # If the main table does not exist, create it by copying the staging table
        query = f"""
        CREATE TABLE {main_table_id} AS
        SELECT * FROM {staging_table_id}
        """
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete.
    
    except ValueError as e:
        print(f"Error: {e}")
        # Handle the case when Unique_ID is missing from one of the tables

    
# Delete staging table created by deduplicate_data()
def clean_up_staging_table():
    dataset_id = "junction-health-data.weinfuse"  
    tables = client.list_tables(dataset_id)  
    
    # Iterate through the tables in the dataset and delete staging tables with unique names
    for table in tables:
        table_id = table.table_id
        if "staging" in table_id:  
            full_table_id = f"{dataset_id}.{table_id}"
            client.delete_table(full_table_id, not_found_ok=True)
            print(f"Deleted table: {full_table_id}")
