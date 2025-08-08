import logging
from pathlib import Path
import sys
from ydata_profiling import ProfileReport

# Add the lib directory to Python path if needed
sys.path.append('lib')

# Import custom logger
from lib.logger import setup_logger, CustomLogger

# Import your file loading module
try:
    from lib import file_load
except ImportError as e:
    print(f"Error importing file_load module: {e}")
    sys.exit(1)

def main():
    """Main application function."""
    
    # Initialize logger
    logger = setup_logger(
        logger_name="SalesDataApp",
        log_folder="logs",
        log_level=logging.INFO
    )
    
    # Alternative way using CustomLogger class directly:
    # custom_logger = CustomLogger("SalesDataApp", "logs", logging.INFO)
    # logger = custom_logger.get_logger()
    
    logger.info("="*50)
    logger.info("APPLICATION STARTED")
    logger.info("="*50)
    
    try:
        # Log the start of file listing operation
        logger.info("Starting CSV file listing operation")
        folder_path = 'data'
        
        # List CSV files
        logger.info(f"Searching for CSV files in folder: {folder_path}")
        files = file_load.list_csv_files(folder_path=folder_path)
        
        if files:
            logger.info(f"Found {len(files)} CSV files:")
            for i, (filename, mod_time) in enumerate(files, 1):
                logger.info(f"  {i}. {filename} (modified: {mod_time})")
            print(f"Files found: {files}")
        else:
            logger.warning(f"No CSV files found in folder: {folder_path}")
            print("No CSV files found")
            return
        
        # Log the start of file reading operation
        logger.info("Starting CSV file reading operation")
        
        # Read the latest CSV file
        logger.info(f"Reading latest CSV file from folder: {folder_path}")
        df = file_load.read_latest_csv(folder_path=folder_path, encoding="latin1")
        
        # Log DataFrame information
        logger.info("CSV file successfully loaded")
        logger.info(f"DataFrame shape: {df.shape}")
        logger.info(f"DataFrame columns: {list(df.columns)}")
        logger.info(f"DataFrame memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # Display first few rows
        print("\nFirst 5 rows of the dataset:")
        print(df.head())

        # Generate a profile report
        logger.info("Generating profile report")
        profile = ProfileReport(df, title="Sales Data Profile Report")
        profile.to_file("reports/sales_data_profile_report.html")
        logger.info("Profile report generated successfully")
        print("Profile report generated: reports/sales_data_profile_report.html")

        # Log some basic statistics
        logger.info("Basic dataset statistics:")
        logger.info(f"  - Total rows: {len(df)}")
        logger.info(f"  - Total columns: {len(df.columns)}")
        logger.info(f"  - Missing values: {df.isnull().sum().sum()}")
        
        # Log data types
        logger.debug("Column data types:")
        for col, dtype in df.dtypes.items():
            logger.debug(f"  - {col}: {dtype}")
        
        logger.info("Data loading and initial analysis completed successfully")
        
    except FileNotFoundError as e:
        logger.error(f"File not found error: {e}")
        print(f"Error: {e}")
    except ValueError as e:
        logger.error(f"Value error: {e}")
        print(f"Error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}", exc_info=True)
        print(f"Unexpected error: {e}")
    
    finally:
        logger.info("="*50)
        logger.info("APPLICATION FINISHED")
        logger.info("="*50)

if __name__ == "__main__":
    main()