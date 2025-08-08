import os
from supabase import create_client, Client
from postgrest import APIResponse

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
