import pandas as pd
from pathlib import Path
import logging
from typing import List, Optional

# Get a logger instance
logger = logging.getLogger(__name__)

def save_df_to_csv(df: pd.DataFrame, file_path: str, file_name: str):
    """
    Saves a DataFrame to a CSV file.

    Parameters:
    -----------
    df : pd.DataFrame
        The DataFrame to save.
    file_path : str
        The relative path to the folder where the file will be saved.
    file_name : str
        The name of the output CSV file (e.g., 'dim_customer.csv').
    """
    try:
        # Create the directory if it doesn't exist
        output_dir = Path(file_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Construct the full file path
        full_path = output_dir / file_name
        
        # Save the DataFrame to CSV
        df.to_csv(full_path, index=False, encoding='utf-8')
        
        logger.info(f"Successfully saved DataFrame to '{full_path}'")
        print(f"Successfully saved DataFrame to '{full_path}'")
        
    except Exception as e:
        logger.error(f"Error saving DataFrame to '{file_name}': {e}", exc_info=True)
        print(f"Error saving DataFrame to '{file_name}': {e}")
        raise

def transform_and_clean(df: pd.DataFrame, columns: List[str], 
                        rename_map: Optional[dict] = None, 
                        distinct_subset: Optional[List[str]] = None) -> pd.DataFrame:
    """
    A generic function to select, clean, rename, and find distinct rows.

    Parameters:
    -----------
    df : pd.DataFrame
        The input DataFrame.
    columns : List[str]
        The list of columns to select from the DataFrame.
    rename_map : dict, optional
        A dictionary to rename columns. {'OLD_NAME': 'new_name'}.
    distinct_subset : List[str], optional
        A list of columns to consider for finding unique rows. If None, all columns are used.

    Returns:
    --------
    pd.DataFrame
        The transformed DataFrame.
    """
    # Select the required columns
    transformed_df = df[columns].copy()
    
    # Get distinct rows
    if distinct_subset:
        transformed_df.drop_duplicates(subset=distinct_subset, inplace=True)
    else:
        transformed_df.drop_duplicates(inplace=True)
        
    # Rename columns if a map is provided
    if rename_map:
        transformed_df.rename(columns=rename_map, inplace=True)
        
    logger.info(f"Transformation complete. Shape: {transformed_df.shape}")
    return transformed_df

def create_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """Creates the Date Dimension DataFrame."""
    logger.info("Creating Date Dimension...")
    
    date_df = df[['ORDERDATE']].copy()
    date_df.drop_duplicates(inplace=True)
    
    # Convert to datetime objects
    date_df['order_date'] = pd.to_datetime(date_df['ORDERDATE'])
    
    # Create date components
    date_df['date_key'] = date_df['order_date'].dt.strftime('%Y%m%d').astype(int)
    date_df['year'] = date_df['order_date'].dt.year
    date_df['quarter'] = date_df['order_date'].dt.quarter
    date_df['month'] = date_df['order_date'].dt.month
    date_df['day'] = date_df['order_date'].dt.day
    
    return date_df[['date_key', 'order_date', 'year', 'quarter', 'month', 'day']]

def create_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    """Creates the Product Dimension DataFrame."""
    logger.info("Creating Product Dimension...")
    
    columns = ['PRODUCTCODE', 'PRODUCTLINE', 'MSRP']
    rename_map = {
        'PRODUCTCODE': 'product_code',
        'PRODUCTLINE': 'product_line',
        'MSRP': 'msrp'
    }
    # Note: PRODUCT_NAME is in the schema but not in the source CSV.
    # It can be added later if a source is available.
    return transform_and_clean(df, columns, rename_map, distinct_subset=['PRODUCTCODE'])

def create_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    """Creates the Customer Dimension DataFrame."""
    logger.info("Creating Customer Dimension...")
    
    columns = [
        'CUSTOMERNAME', 'CONTACTFIRSTNAME', 'CONTACTLASTNAME', 'PHONE', 
        'ADDRESSLINE1', 'ADDRESSLINE2', 'CITY', 'STATE', 'POSTALCODE', 
        'COUNTRY', 'TERRITORY'
    ]
    rename_map = {
        'CUSTOMERNAME': 'customer_name',
        'CONTACTFIRSTNAME': 'contact_first_name',
        'CONTACTLASTNAME': 'contact_last_name',
        'PHONE': 'phone',
        'ADDRESSLINE1': 'addressline1',
        'ADDRESSLINE2': 'addressline2',
        'CITY': 'city',
        'STATE': 'state',
        'POSTALCODE': 'postalcode',
        'COUNTRY': 'country',
        'TERRITORY': 'territory'
    }
    return transform_and_clean(df, columns, rename_map, distinct_subset=['CUSTOMERNAME'])

def create_dim_order(df: pd.DataFrame) -> pd.DataFrame:
    """Creates the Order Dimension DataFrame."""
    logger.info("Creating Order Dimension...")
    
    columns = ['ORDERNUMBER', 'STATUS']
    rename_map = {'ORDERNUMBER': 'order_number', 'STATUS': 'status'}
    return transform_and_clean(df, columns, rename_map, distinct_subset=['ORDERNUMBER'])

def create_fact_sales(df: pd.DataFrame, dim_date_df: pd.DataFrame) -> pd.DataFrame:
    """Creates the Sales Fact Table DataFrame."""
    logger.info("Creating Sales Fact Table...")
    
    # Merge with date dimension to get the date_key
    fact_df = pd.merge(
        df, 
        dim_date_df[['order_date', 'date_key']], 
        left_on=pd.to_datetime(df['ORDERDATE']), 
        right_on='order_date',
        how='left'
    )
    
    # Select and rename columns for the fact table
    columns = [
        'ORDERNUMBER', 'PRODUCTCODE', 'CUSTOMERNAME', 'date_key',
        'SALES', 'QUANTITYORDERED', 'PRICEEACH', 'DEALSIZE', 'ORDERLINENUMBER'
    ]
    rename_map = {
        'ORDERNUMBER': 'order_number',
        'PRODUCTCODE': 'product_code',
        'CUSTOMERNAME': 'customer_name',
        'SALES': 'sales',
        'QUANTITYORDERED': 'quantity_ordered',
        'PRICEEACH': 'price_each',
        'DEALSIZE': 'deal_size',
        'ORDERLINENUMBER': 'order_line_number'
    }
    
    final_fact_df = fact_df[columns].copy()
    final_fact_df.rename(columns=rename_map, inplace=True)
    
    # sales_id is a serial key, so we don't generate it here.
    # The database will handle it upon insertion.
    
    logger.info(f"Sales Fact Table created. Shape: {final_fact_df.shape}")
    return final_fact_df