import pandas as pd
import os
import glob
from pathlib import Path
from typing import Optional, Dict, Any

def read_latest_csv(folder_path: str,
                   file_name: Optional[str] = None,
                   **csv_kwargs: Any) -> pd.DataFrame:
    """
    Read the latest CSV file from a given folder and load it into a pandas DataFrame.

    Parameters:
    -----------
    folder_path : str
        Relative or absolute path to the folder containing CSV files
    file_name : str, optional
        Specific file name to read. If None, reads the latest CSV file in the folder
    **csv_kwargs : Any
        Additional keyword arguments to pass to pd.read_csv() 
        (e.g., sep=',', encoding='utf-8', header=0, etc.)

    Returns:
    --------
    pd.DataFrame
        DataFrame containing the data from the CSV file

    Raises:
    -------
    FileNotFoundError
        If no CSV files are found in the specified folder
    ValueError
        If the specified file_name doesn't exist or folder path is invalid
    """

    # Convert to Path object for better path handling
    folder_path = Path(folder_path)

    # Check if folder exists
    if not folder_path.exists():
        raise ValueError(f"Folder path '{folder_path}' does not exist")

    if not folder_path.is_dir():
        raise ValueError(f"'{folder_path}' is not a directory")

    try:
        if file_name:
            # If specific file name is provided
            file_path = folder_path / file_name
            if not file_path.exists():
                raise ValueError(f"File '{file_name}' not found in '{folder_path}'")

            # Check if it's a CSV file
            if not file_path.suffix.lower() == '.csv':
                raise ValueError(f"'{file_name}' is not a CSV file")

            csv_file = file_path
        else:
            # Find all CSV files in the folder
            csv_files = list(folder_path.glob('*.csv'))

            if not csv_files:
                raise FileNotFoundError(f"No CSV files found in '{folder_path}'")

            # Get the latest file based on modification time
            csv_file = max(csv_files, key=lambda x: x.stat().st_mtime)
            print(f"Reading latest CSV file: {csv_file.name}")

        # Read the CSV file into DataFrame
        df = pd.read_csv(csv_file, **csv_kwargs)

        print(f"Successfully loaded data from '{csv_file.name}'")
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")

        return df

    except Exception as e:
        print(f"Error reading CSV file: {str(e)}")
        raise

def list_csv_files(folder_path: str) -> list:
    """
    List all CSV files in a given folder with their modification times.
    
    Parameters:
    -----------
    folder_path : str
        Path to the folder to search
    
    Returns:
    --------
    list
        List of tuples containing (file_name, modification_time)
    """
    folder_path = Path(folder_path)
    
    if not folder_path.exists():
        print(f"Folder '{folder_path}' does not exist")
        return []
    
    csv_files = list(folder_path.glob('*.csv'))
    
    # Sort by modification time (newest first)
    file_info = [(f.name, f.stat().st_mtime) for f in csv_files]
    file_info.sort(key=lambda x: x[1], reverse=True)
    
    return file_info