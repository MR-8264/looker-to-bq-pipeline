from google.cloud import bigquery
import looker_sdk
sdk = looker_sdk.init40()
client = bigquery.Client()

def main():
    # Add names of Looks to transfer to BigQuery
    look_names = ["Saved Look 1", "Saved Look 2"]

    # Run these functions for each Look in list
    for look_name in look_names:
        print(f"Starting transfer of {look_name}")
        look_id = get_look_id(look_name)
        data = get_data_from_looker(look_id)
        write_to_file(data)
        table_name = get_bq_table_name(look_name)
        create_table(table_name)
        load_to_bq(table_name)
        remove_temp()

def get_look_id(name):
    results = sdk.search_looks(title=name, fields="id")
    print(f"Look ID found: {results [0].id}")
    return results [0].id

def get_data_from_looker(look_id):
    response = sdk.run_look(
        look_id=look_id,
        result_format="csv")
    print(f"Data successfully gathered from Look {look_id}")
    return response

def write_to_file(data):
  # Transform the columns' name (i.e: "User ID" to become "User_ID") because 
  # BigQuery does not accept a white space inside columns' name 
  cnt = 0 # cnt is to find the index of the character after the last character of columns'names
  for i in data: 
    if i == "\n":
        break
    else: 
        cnt += 1
  header = data[:cnt]
  header_to_write = header.replace(" ", "_")
  data_to_write = data[cnt:]
  # Write header and data to temporary disk
  with open('/tmp/table.csv', "w") as csv: # Files can only be modified/written inside tmp/
    csv.write(header_to_write)
    csv.write(data_to_write)
  print("Successfully wrote data to a CSV file stored in temporary disk")

def get_bq_table_name(look_name):
   bq_name = look_name.replace(" ", "_")
   bq_name = bq_name.lower()
   print(f"'{look_name}' successfully changed to '{bq_name}'")
   return bq_name

def create_table(name: str) -> None:
    table_id = f"project_id.dataset_id.{name}" #Replace with actual ids
    try:
        client.get_table(table_id)
        print(f"Table {table_id} exists in BiqQuery")
    except Exception:
        table = bigquery.Table(table_id)
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )
        
def load_to_bq(table_name):
    table_id = f"project_id.dataset_id.{table_name}" #Replace with actual ids
    temp_table_id = f"{table_id}_temp"

    # Load CSV into temporary table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, 
        skip_leading_rows=1, 
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    with open("/tmp/table.csv", "rb") as source_file:
        job = client.load_table_from_file(source_file, temp_table_id, job_config=job_config)
    job.result()

    # Get schema from temp table
    temp_table = client.get_table(temp_table_id)
    temp_schema = temp_table.schema  

    # Check if main table exists
    main_table = None
    try:
        main_table = client.get_table(table_id)
    except Exception:
        pass  

    # If table is missing or has no schema, recreate it
    if main_table is None or not main_table.schema:
        client.delete_table(table_id, not_found_ok=True)  # Remove empty table if needed
        new_table = bigquery.Table(table_id, schema=temp_schema)
        client.create_table(new_table)
        print(f"Created table {table_id} with schema from temp table.")

    # Get unique identifier column (assumed first column)
    unique_column = temp_schema[0].name  
    
    # Merge new records only
    merge_query = f"""
    MERGE `{table_id}` AS target
    USING `{temp_table_id}` AS source
    ON target.{unique_column} = source.{unique_column}
    WHEN NOT MATCHED THEN
      INSERT ROW
    """
    client.query(merge_query).result()  # Execute query

    print(f"Loaded new unique rows into {table_id} from {temp_table_id}")

def remove_temp():
    dataset_id = "project_id.dataset_id"  #Replace with actual ids

    # List all tables in the dataset
    tables = client.list_tables(dataset_id)

    for table in tables:
        if "temp" in table.table_id:
            client.delete_table(f"{dataset_id}.{table.table_id}", not_found_ok=True)
            print(f"Deleted temporary table: {table.table_id}")

if __name__ == "__main__":
    main()