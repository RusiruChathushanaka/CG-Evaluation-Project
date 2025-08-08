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

if __name__ == "__main__":
    try:
        supabase = get_supabase_client()
        print("Successfully connected to Supabase!")

        portfolio_data = get_table_data(supabase, "rusiru_portfolio_site_projectsdata")
        if portfolio_data:
            print("\nFirst 2 portfolio records:")
            for record in portfolio_data[:2]:
                print(record)

        print("\n" + "="*40)
    except Exception as e:
        print(f"Failed to connect to Supabase: {e}")
