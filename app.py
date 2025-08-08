import logging
from pathlib import Path
import sys
from ydata_profiling import ProfileReport

# Add the lib directory to Python path if needed
sys.path.append('lib')

# Import custom logger
from lib.logger import setup_logger, CustomLogger


try:
    from lib import file_load
except ImportError as e:
    print(f"Error importing file_load module: {e}")
    sys.exit(1)

try:
    from lib import data_transform
except ImportError as e:
    print(f"Error importing data_transform module: {e}")
    sys.exit(1)

def main():
    """Main application function."""
    
    # Initialize logger
    logger = setup_logger(
        logger_name="SalesDataApp",
        log_folder="logs",
        log_level=logging.INFO
    )
    

    
    logger.info("="*50)
    logger.info("APPLICATION STARTED")
    logger.info("="*50)
    
    try:
        # --- 1. DATA LOADING ---
        logger.info("--- Starting Data Loading Stage ---")
        source_folder = 'data'
        raw_df = file_load.read_latest_csv(folder_path=source_folder, encoding="latin1")
        logger.info("Raw data loaded successfully.")
        logger.info(f"Raw DataFrame shape: {raw_df.shape}")

        # Generate a profile report
        logger.info("Generating profile report")
        profile = ProfileReport(raw_df, title="Sales Data Profile Report")
        profile.to_file("reports/sales_data_profile_report.html")
        logger.info("Profile report generated successfully")
        print("Profile report generated: reports/sales_data_profile_report.html")

        # --- 2. DATA TRANSFORMATION ---
        logger.info("--- Starting Data Transformation Stage ---")
        
        # Create dimension tables
        dim_date_df = data_transform.create_dim_date(raw_df)
        dim_product_df = data_transform.create_dim_product(raw_df)
        dim_customer_df = data_transform.create_dim_customer(raw_df)
        dim_order_df = data_transform.create_dim_order(raw_df)
        
        # Create fact table
        fact_sales_df = data_transform.create_fact_sales(raw_df, dim_date_df)
        
        logger.info("All tables created successfully in memory.")


        # --- 3. DATA SAVING ---
        logger.info("--- Starting Data Saving Stage ---")
        output_folder = 'transformed_data'
        
        data_transform.save_df_to_csv(dim_date_df, output_folder, 'dim_date.csv')
        data_transform.save_df_to_csv(dim_product_df, output_folder, 'dim_product.csv')
        data_transform.save_df_to_csv(dim_customer_df, output_folder, 'dim_customer.csv')
        data_transform.save_df_to_csv(dim_order_df, output_folder, 'dim_order.csv')
        data_transform.save_df_to_csv(fact_sales_df, output_folder, 'fact_sales.csv')
        
        logger.info(f"All transformed data saved to '{output_folder}' directory.")

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