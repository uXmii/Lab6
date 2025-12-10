Lab 6 - TFX Iterative Schema with Custom Modifications

## Overview
This lab demonstrates how to work with iterative schema development using TensorFlow Extended (TFX) and ML Metadata. This is a modified version of the original Jupyter notebook lab, converted to a modular Python project with custom enhancements for better understanding and practical application.
 Key Modifications Made
 Structure & Format Changes

Converted from Jupyter notebook to modular Python scripts
VS Code compatible project structure
Separated concerns into different modules
Added comprehensive error handling and logging

# Enhanced Features

Custom age validation (restricted to 17-90 years instead of default)
Enhanced schema management with additional validations
Automated pipeline execution with better control flow
Improved metadata tracking capabilities
Schema visualization enhancements

# Code Quality Improvements

Modular architecture for better maintainability
Professional logging system
Error handling throughout the pipeline
Documentation and type hints

# Project Structure

Lab6/
├── README.md                  # This file
├── requirements.txt           # Python dependencies
├── main.py                   # Entry point
├── src/                      # Source code modules
│   ├── __init__.py
│   ├── data_pipeline.py      # Main TFX pipeline components
│   ├── schema_manager.py     # Schema customization & management
│   ├── metadata_tracker.py   # ML Metadata exploration
│   └── utils.py             # Utility functions
├── data/                    # Data directory
│   └── census_data/
│       └── adult.data       # Census Income dataset
└── pipeline/                # Pipeline artifacts (auto-generated)
 Quick Start
Prerequisites

Python 3.7+
TensorFlow 2.x
Git

# Installation & Setup
bash# 1. Clone the repository
git clone https://github.com/uXmii/Lab6.git
cd Lab6

# 2. Create virtual environment (recommended)
python -m venv venv

# Windows
.\venv\Scripts\activate

# macOS/Linux  
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the lab
python main.py
 Usage Examples
Basic Pipeline Execution
pythonfrom src.data_pipeline import TFXPipeline

# Initialize pipeline
pipeline = TFXPipeline(data_root='./data/census_data')

# Run complete pipeline
pipeline.run_full_pipeline()

# Display results
pipeline.display_results()
Custom Schema Management
pythonfrom src.schema_manager import SchemaManager

# Load and modify schema
schema_manager = SchemaManager()
schema = schema_manager.load_schema('path/to/schema.pbtxt')

# Apply custom modifications
schema = schema_manager.customize_schema(schema)

# Restrict age domain (custom modification)
schema_manager.customize_age_domain(schema, min_age=18, max_age=85)

# Add training/serving environments
schema_manager.add_environments(schema, ['TRAINING', 'SERVING'])
Metadata Exploration
pythonfrom src.metadata_tracker import MetadataTracker

# Initialize tracker
tracker = MetadataTracker(context)

# Track artifact lineage
lineage = tracker.track_example_anomalies_lineage()
print(f"Input artifacts: {lineage['input_ids']}")

# Display metadata summary
tracker.display_artifact_summary()
🔧 Key Components
1. Data Pipeline (src/data_pipeline.py)

ExampleGen: Data ingestion from CSV
StatisticsGen: Data profiling and statistics
SchemaGen: Automatic schema inference
ImportSchemaGen: Import curated schemas
ExampleValidator: Data validation against schema

2. Schema Manager (src/schema_manager.py)

Schema loading and modification
Domain restrictions (age: 17-90 years)
Environment setup (TRAINING/SERVING)
Validation rules and export functionality

3. Metadata Tracker (src/metadata_tracker.py)

Artifact lineage tracking
Execution history analysis
Metadata store exploration
Lineage visualization

4. Utils (src/utils.py)

Logging setup
Directory management
Data validation
Helper functions

 Dataset Information

Source: UCI Adult Income Dataset
Features: Age, workclass, education, marital status, occupation, etc.
Target: Income level (>50K or ≤50K)
Size: ~32K records with 15 attributes

 Custom Modifications Implemented
1. Enhanced Age Validation
python# Original: No domain restrictions

# Modified: Age restricted to realistic range
tfdv.set_domain(schema, 'age', schema_pb2.IntDomain(name='age', min=17, max=90))
2. Environment Configuration
python# Added separate training/serving environments
schema.default_environment.append('TRAINING')
schema.default_environment.append('SERVING')
tfdv.get_feature(schema, 'label').not_in_environment.append('SERVING')
3. Automated Reporting

Generate validation reports automatically
Enhanced error messages and logging
Pipeline execution summaries

4. Modular Architecture

Separated into logical modules
Better code organization
Reusable components

# Pipeline Execution Flow
mermaidgraph TD
    A[Raw Data] --> B[ExampleGen]
    B --> C[StatisticsGen]
    C --> D[SchemaGen]
    D --> E[Schema Customization]
    E --> F[ImportSchemaGen]
    C --> G[ExampleValidator]
    F --> G
    G --> H[Validation Results]
    
    style E fill:#f9f,stroke:#333,stroke-width:2px
    style H fill:#9f9,stroke:#333,stroke-width:2px
    
# Learning Objectives

 TFX pipeline components and their interactions
 Schema inference and customization techniques
 ML Metadata for artifact tracking and lineage
 Data validation and anomaly detection
 Pipeline orchestration best practices
 Environment management for training vs serving

# Troubleshooting
Common Issues & Solutions
IssueCauseSolutionImport ErrorsMissing dependenciespip install -r requirements.txtData Not FoundDataset missingDownload from UCI ML RepositoryPermission ErrorsFile access issuesCheck file/directory permissionsMemory IssuesLarge datasetUse smaller sample for testingTFX Version ConflictsVersion mismatchUse specified versions in requirements.txt
Dataset Download
If automatic download fails:
bash# Manual download
wget https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data -O data/census_data/adult.data

# Future Enhancements
Potential improvements for further learning:

 Add Transform component for feature engineering
 Implement Trainer component for model training
 Add Evaluator for model validation
 Create Pusher for model deployment
 Add data drift detection
 Implement continuous integration pipeline
