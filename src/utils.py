"""
Utility functions for TFX Pipeline
"""

import os
import logging
from pathlib import Path

def setup_logging(level=logging.INFO):
    """
    Setup logging configuration
    
    Args:
        level: Logging level (default: INFO)
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('tfx_pipeline.log')
        ]
    )

def create_directories(directories):
    """
    Create directories if they don't exist
    
    Args:
        directories: List of directory paths to create
    """
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)

def validate_data_file(filepath):
    """
    Validate that a data file exists and is readable
    
    Args:
        filepath: Path to the data file
        
    Returns:
        bool: True if valid, False otherwise
    """
    try:
        if not os.path.exists(filepath):
            return False
        
        if not os.access(filepath, os.R_OK):
            return False
        
        # Check if file is not empty
        if os.path.getsize(filepath) == 0:
            return False
        
        return True
        
    except Exception:
        return False

def get_file_size_mb(filepath):
    """
    Get file size in MB
    
    Args:
        filepath: Path to the file
        
    Returns:
        float: File size in MB
    """
    try:
        size_bytes = os.path.getsize(filepath)
        return size_bytes / (1024 * 1024)
    except Exception:
        return 0.0

def clean_directory(directory, keep_files=None):
    """
    Clean directory contents
    
    Args:
        directory: Directory to clean
        keep_files: List of files to keep (optional)
    """
    if keep_files is None:
        keep_files = []
    
    try:
        if os.path.exists(directory):
            for filename in os.listdir(directory):
                if filename not in keep_files:
                    filepath = os.path.join(directory, filename)
                    if os.path.isfile(filepath):
                        os.remove(filepath)
                    elif os.path.isdir(filepath):
                        import shutil
                        shutil.rmtree(filepath)
    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to clean directory {directory}: {str(e)}")

def format_bytes(bytes_value):
    """
    Format bytes into human-readable format
    
    Args:
        bytes_value: Number of bytes
        
    Returns:
        str: Formatted string (e.g., "1.5 MB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"

def print_section_header(title, width=60):
    """
    Print a formatted section header
    
    Args:
        title: Title of the section
        width: Width of the header line
    """
    print("\n" + "=" * width)
    print(f" {title} ")
    print("=" * width)

def print_step_info(step_number, step_name, description=""):
    """
    Print formatted step information
    
    Args:
        step_number: Step number
        step_name: Name of the step
        description: Optional description
    """
    print(f"\n>>> Step {step_number}: {step_name}")
    if description:
        print(f"    {description}")

def download_census_data(data_root):
    """
    Download Census Income dataset if not present
    
    Args:
        data_root: Directory to save the data
    """
    import urllib.request
    
    data_url = "https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data"
    data_file = os.path.join(data_root, "adult.data")
    
    if not os.path.exists(data_file):
        print(f"Downloading Census Income dataset to {data_file}...")
        try:
            os.makedirs(data_root, exist_ok=True)
            urllib.request.urlretrieve(data_url, data_file)
            print(f"Dataset downloaded successfully!")
            return True
        except Exception as e:
            print(f"Failed to download dataset: {str(e)}")
            return False
    else:
        print(f"Dataset already exists at {data_file}")
        return True