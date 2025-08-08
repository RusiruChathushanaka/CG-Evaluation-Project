import os
from supabase import create_client, Client
from postgrest import APIResponse
import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load Supabase URL and API key from environment variables for security
SUPABASE_URL = "https://rswtvogsljeupyoxdeww.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJzd3R2b2dzbGpldXB5b3hkZXd3Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDUzMTI4MzUsImV4cCI6MjA2MDg4ODgzNX0.U69RQ0OYap9KtqzdWgDTGztPFhCP6qoLIvA5dGajU8k"

def get_supabase_client() -> Client:
    """
    Initializes and returns a Supabase client.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Supabase URL or API key not set in environment variables.")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return supabase

# --------------------------------------------------------------------------
# 1. Function to Get Table Data
# --------------------------------------------------------------------------
def get_table_data(client: Client, table_name: str) -> list:
    """
    Fetches all rows and columns from a specified table.

    Args:
        client: The initialized Supabase client.
        table_name: The name of the table to fetch data from.

    Returns:
        A list of dictionaries, where each dictionary is a row.
        Returns an empty list if the table is empty or an error occurs.
    """
    try:
        print(f"Attempting to fetch data from table: '{table_name}'...")
        response: APIResponse = client.from_(table_name).select("*").execute()
        
        if response.data:
            print(f"Successfully fetched {len(response.data)} rows.")
            return response.data
        else:
            print(f"No data returned for table '{table_name}'. It might be empty or check permissions.")
            return []
            
    except Exception as e:
        print(f"An error occurred while fetching data from '{table_name}': {e}")
        return []
    

# --------------------------------------------------------------------------
# Function to Get Table Columns
# --------------------------------------------------------------------------
def get_table_columns(client: Client, table_name: str) -> list:
    """
    Fetches the column names of a specified table by inspecting its first row.

    Args:
        client: The initialized Supabase client.
        table_name: The name of the table to fetch columns from.

    Returns:
        A list of column names.
        Returns an empty list if the table is empty, not found, or an error occurs.
    """
    try:
        print(f"Attempting to get columns for table: '{table_name}'...")

        response: APIResponse = client.from_(table_name).select("*").limit(1).execute()

        if response.data:

            columns = list(response.data[0].keys())
            print(f"Successfully retrieved columns: {columns}")
            return columns
        else:
            print(f"Could not retrieve columns for '{table_name}'. The table may be empty or inaccessible.")
            return []

    except Exception as e:
        print(f"An error occurred while fetching columns for '{table_name}': {e}")
        return []
    
# --------------------------------------------------------------------------
#  Function to Delete All Records from a Table
# --------------------------------------------------------------------------
def delete_all_records(client: Client, table_name: str) -> list:
    """
    Deletes all records from a table and returns the data that was deleted.

    THIS IS A DESTRUCTIVE OPERATION. USE WITH EXTREME CAUTION.

    Args:
        client: The initialized Supabase client.
        table_name: The name of the table to clear.

    Returns:
        A list of dictionaries for the records that were deleted.
        Returns an empty list if the table was already empty or an error occurred.
    """
    try:
        print(f"--- WARNING: Attempting to delete ALL records from table: '{table_name}' ---")


        columns = get_table_columns(client, table_name)
        if not columns:
            print(f"Table '{table_name}' is either empty or could not be accessed. No records to delete.")
            return []

        filter_column = columns[0]

        print(f"Executing DELETE on '{table_name}' using filter on column '{filter_column}'...")
        response: APIResponse = client.from_(table_name).delete().neq(filter_column, "IMPOSSIBLE_VALUE_TO_MATCH").execute()

        if response.data:
            print(f"Successfully deleted {len(response.data)} records.")
            return response.data
        else:
            print(f"No records were deleted. The table may have been empty or check RLS policies for DELETE permissions.")
            return []

    except Exception as e:
        print(f"An error occurred while deleting records from '{table_name}': {e}")
        return []
    

# --------------------------------------------------------------------------
#  Function to Upload a DataFrame to a Supabase Table
# --------------------------------------------------------------------------
def upload_df_to_supabase(client: Client, df: pd.DataFrame, table_name: str, batch_size: int = 1000) -> bool:
    """
    Deletes all existing data and uploads a DataFrame to a Supabase table in batches.
    This version is more resilient and will proceed with an upload even if the initial delete fails.

    Args:
        client: The initialized Supabase client.
        df: The pandas DataFrame to upload.
        table_name: The name of the destination table in Supabase.
        batch_size: The number of rows to insert in each batch.

    Returns:
        True if the upload was successful, False otherwise.
    """
    logger.info(f"Starting upload process for table '{table_name}' with {len(df)} rows.")

    # 1. Attempt to delete all existing records.
    # This is now a "best-effort" step. If it fails, we log a warning and continue.
    # The subsequent insert will act as the definitive success/fail check.
    if not delete_all_records(client, table_name):
        logger.warning(
            f"Could not clear table '{table_name}' before upload. "
            f"Proceeding with insert anyway. This may fail if there are duplicate primary keys."
        )

    # 2. Prepare the DataFrame for upload
    # Replace pandas NaN with None for database compatibility
    df_clean = df.replace({np.nan: None})
    
    # Convert date columns to string format 'YYYY-MM-DD' if they exist
    if 'order_date' in df_clean.columns:
        df_clean['order_date'] = pd.to_datetime(df_clean['order_date']).dt.strftime('%Y-%m-%d')

    # Convert DataFrame to a list of dictionaries
    records = df_clean.to_dict(orient='records')
    
    if not records:
        logger.warning(f"DataFrame for '{table_name}' is empty. Nothing to upload.")
        return True

    # 3. Insert data in batches
    total_rows = len(records)
    for i in range(0, total_rows, batch_size):
        batch = records[i:i + batch_size]
        logger.info(f"Uploading batch {i//batch_size + 1}: rows {i+1} to {min(i+batch_size, total_rows)} for '{table_name}'.")
        
        try:
            response: APIResponse = client.from_(table_name).insert(batch).execute()
            
            # The API response for an insert should contain a list of the inserted records.
            # If the length of the response data doesn't match the batch size, it's an error.
            if len(response.data) != len(batch):
                 logger.error(f"Upload failed for batch starting at row {i} for table '{table_name}'. Response: {response}")
                 return False

        except Exception as e:
            logger.error(f"An exception occurred during batch insert for '{table_name}': {e}", exc_info=True)
            return False
            
    logger.info(f"Successfully uploaded all {total_rows} rows to '{table_name}'.")
    return True

if __name__ == "__main__":
    try:
        supabase_client = get_supabase_client()
        print("Successfully connected to Supabase!")

        print("\n" + "="*40)
        
        table_to_modify = "cg_dim_customer" 

        # --- Example 1: Get current data ---
        print(f"Getting current data from '{table_to_modify}'...")
        current_data = get_table_data(supabase_client, table_to_modify)
        if current_data:
            print(f"Found {len(current_data)} records. Showing first one: {current_data[0]}")
        else:
            print("Table is currently empty.")

        # --- Example 2: Delete all records (use with caution!) ---
        #
        # UNCOMMENT THE FOLLOWING LINES TO RUN THE DELETE OPERATION
        #
        print("\n" + "="*40)
        print("Running delete operation...")
        deleted_data = delete_all_records(supabase_client, table_to_modify)
        if deleted_data:
            print(f"\nData that was deleted ({len(deleted_data)} records):")
            # Print the first deleted record as a sample
            print(deleted_data[0])
        
            # Verify the table is now empty
            print("\nVerifying table is empty...")
            final_data = get_table_data(supabase_client, table_to_modify)
            if not final_data:
                print(f"Verification successful: Table '{table_to_modify}' is now empty.")

    except Exception as e:
        print(f"An error occurred in the main script: {e}")
