#!/usr/bin/env python3
"""
Main entry point for TFX Iterative Schema Lab
Author: Custom Implementation based on TFX Tutorial
Description: Enhanced version with modular design and custom modifications
"""

import os
import sys
import logging
from pathlib import Path

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.data_pipeline import TFXPipeline
from src.utils import setup_logging, create_directories

def main():
    """Main execution function"""
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting TFX Iterative Schema Lab - Enhanced Version")
    
    try:
        # Define paths
        pipeline_root = './pipeline/'
        data_root = './data/census_data'
        
        # Create necessary directories
        create_directories([pipeline_root, data_root])
        
        # Check if data exists
        data_file = os.path.join(data_root, 'adult.data')
        if not os.path.exists(data_file):
            logger.warning(f"Data file not found at {data_file}")
            logger.info("Please download the Census Income dataset from:")
            logger.info("https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data")
            logger.info(f"Save it as {data_file}")
            return
        
        # Initialize pipeline
        logger.info("Initializing TFX Pipeline...")
        pipeline = TFXPipeline(
            pipeline_root=pipeline_root,
            data_root=data_root
        )
        
        # Run the complete pipeline
        logger.info("Running complete pipeline...")
        pipeline.run_full_pipeline()
        
        # Display results
        logger.info("Pipeline completed successfully!")
        pipeline.display_results()
        
        # Demonstrate metadata tracking
        logger.info("Demonstrating metadata tracking...")
        pipeline.demonstrate_metadata_tracking()
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise
    
    logger.info("Lab completed successfully!")

if __name__ == "__main__":
    main()