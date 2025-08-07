import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

class CustomLogger:
    """
    Custom logger class that creates timestamped log files and handles directory creation.
    """
    
    def __init__(self, 
                 logger_name: str = "AppLogger",
                 log_folder: str = "logs",
                 log_level: int = logging.INFO):
        """
        Initialize the custom logger.
        
        Parameters:
        -----------
        logger_name : str
            Name of the logger
        log_folder : str
            Folder where log files will be stored
        log_level : int
            Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.logger_name = logger_name
        self.log_folder = Path(log_folder)
        self.log_level = log_level
        self.logger = None
        
        # Create the logger
        self._setup_logger()
    
    def _create_log_directory(self):
        """Create log directory if it doesn't exist."""
        try:
            self.log_folder.mkdir(parents=True, exist_ok=True)
            print(f"Log directory created/verified: {self.log_folder}")
        except Exception as e:
            print(f"Error creating log directory: {e}")
            raise
    
    def _generate_log_filename(self) -> str:
        """Generate timestamped log filename."""
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M")
        return f"log_{timestamp}.log"
    
    def _setup_logger(self):
        """Set up the logger with file and console handlers."""
        # Create log directory
        self._create_log_directory()
        
        # Create logger
        self.logger = logging.getLogger(self.logger_name)
        self.logger.setLevel(self.log_level)
        
        # Clear any existing handlers to avoid duplicates
        self.logger.handlers.clear()
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        console_formatter = logging.Formatter(
            fmt='%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        
        # Create file handler
        log_filename = self._generate_log_filename()
        log_filepath = self.log_folder / log_filename
        
        file_handler = logging.FileHandler(log_filepath, mode='w', encoding='utf-8')
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(detailed_formatter)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(console_formatter)
        
        # Add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Log the initialization
        self.logger.info(f"Logger initialized - Log file: {log_filepath}")
        self.logger.info(f"Log level set to: {logging.getLevelName(self.log_level)}")
    
    def get_logger(self) -> logging.Logger:
        """Return the configured logger instance."""
        return self.logger
    
    def log_function_entry(self, func_name: str, **kwargs):
        """Log function entry with parameters."""
        params = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
        self.logger.info(f"ENTRY: {func_name}({params})")
    
    def log_function_exit(self, func_name: str, result: Optional[str] = None):
        """Log function exit with optional result."""
        if result:
            self.logger.info(f"EXIT: {func_name} - {result}")
        else:
            self.logger.info(f"EXIT: {func_name}")
    
    def log_dataframe_info(self, df, df_name: str = "DataFrame"):
        """Log DataFrame information."""
        self.logger.info(f"{df_name} shape: {df.shape}")
        self.logger.info(f"{df_name} columns: {list(df.columns)}")
        self.logger.info(f"{df_name} memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

def setup_logger(logger_name: str = "AppLogger", 
                log_folder: str = "logs", 
                log_level: int = logging.INFO) -> logging.Logger:
    """
    Convenience function to set up and return a logger.
    
    Parameters:
    -----------
    logger_name : str
        Name of the logger
    log_folder : str
        Folder where log files will be stored
    log_level : int
        Logging level
    
    Returns:
    --------
    logging.Logger
        Configured logger instance
    """
    custom_logger = CustomLogger(logger_name, log_folder, log_level)
    return custom_logger.get_logger()

# Global logger instance (optional - for simple usage)
_global_logger = None

def get_global_logger() -> logging.Logger:
    """Get or create global logger instance."""
    global _global_logger
    if _global_logger is None:
        _global_logger = setup_logger()
    return _global_logger