import os
from supabase import create_client, Client

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

if __name__ == "__main__":
    try:
        supabase = get_supabase_client()
        print("Successfully connected to Supabase!")
    except Exception as e:
        print(f"Failed to connect to Supabase: {e}")