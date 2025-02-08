# Transferring Looks from Looker to BigQuery

## Overview
This script automates the process of transferring specific Looks from a Looker instance to Google BigQuery. It:
1. Retrieves Looker Look IDs based on Look names.
2. Extracts Look data as CSV using Looker's API.
3. Transforms and loads the data into BigQuery.
4. Prevents duplicate data using a deduplication process.
5. Cleans up temporary staging tables after processing.

---

## Prerequisites
Before running the script, ensure you have the following:

- Google Cloud SDK Installed and authenticated (`gcloud auth application-default login`).
- BigQuery Access with permission to create and modify tables.
- Looker API Access with `run_look` and `search_looks` permissions.
- A `looker.ini` configuration file with Looker API credentials.

---

## Setting Up the `looker.ini` File
Create a file named `looker.ini` in the root directory of your project.
The file should follow this format:

```
[Looker]
# Base URL for API. Do not include /api/* in the URL
base_url=https://<your-looker-endpoint>:19999

# API client ID and secret (from Looker Admin)
client_id=your_API_client_id
client_secret=your_API_client_secret

# Set to false only if testing locally against self-signed certs
verify_ssl=true

# Timeout for API requests (default: 120 seconds)
timeout=120
```

---

## Installation
1. Clone or download the script into a working directory.
2. Ensure required Python packages are installed:
   ```bash
   pip install google-cloud-bigquery looker-sdk pandas
   ```
3. Authenticate with Google Cloud for BigQuery access:
   ```bash
   gcloud auth application-default login
   ```

---

## Configuration
Update the `looks_to_transfer` variable in the script to match the Looks you want to transfer:
```python
looks_to_transfer = ("Look_Name_01", "Look_Name_02")
```
Ensure these Look names match exactly as they appear in Looker.

---

## Running the Script
To execute the script and transfer the data from Looker to BigQuery:
```bash
python main.py
```
The script will:
- Extract data from Looker.
- Store it in a temporary CSV.
- Load it into BigQuery.
- Deduplicate and merge data to prevent duplication.

---

## How Deduplication Works
- Data is first loaded into a staging table (`junction-health-data.weinfuse.staging`).
- If the main table already exists, the script merges data while preventing duplicate records.
- If the main table does not exist, the staging table is copied as the main table.
- After processing, temporary staging tables are deleted.

---

## Error Handling
- If a Look ID is not found, the script will exit with an error.
- If a column does not exist in BigQuery, it will print an error message.
- The script ensures that each Look’s data is stored separately in BigQuery under a unique table name.

---

## Next Steps & Customization
- Modify `deduplicate_data()` to customize how duplicate records are handled.
- Extend `looks_to_transfer` to include more Looks.
- Schedule the script using a cron job or Google Cloud Functions for automation.

---

## Contact & Support
For any issues, reach out to your Looker admin or check the [Looker API Documentation](https://cloud.google.com/looker/docs/api-intro).
