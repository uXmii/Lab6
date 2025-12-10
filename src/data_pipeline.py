"""
TFX Data Pipeline Implementation
Enhanced version with custom modifications and better error handling
"""

import os
import logging
import tensorflow as tf
import tensorflow_data_validation as tfdv

from tfx import v1 as tfx
from tfx.orchestration.experimental.interactive.interactive_context import InteractiveContext
from tensorflow_metadata.proto.v0 import schema_pb2

from .schema_manager import SchemaManager
from .metadata_tracker import MetadataTracker

class TFXPipeline:
    """Enhanced TFX Pipeline with custom modifications"""
    
    def __init__(self, pipeline_root='./pipeline/', data_root='./data/census_data'):
        """
        Initialize the TFX Pipeline
        
        Args:
            pipeline_root (str): Path to pipeline metadata store
            data_root (str): Path to raw data directory
        """
        self.pipeline_root = pipeline_root
        self.data_root = data_root
        self.data_filepath = os.path.join(data_root, 'adult.data')
        
        self.logger = logging.getLogger(__name__)
        
        # Initialize Interactive Context
        self.context = InteractiveContext(pipeline_root=pipeline_root)
        
        # Initialize managers
        self.schema_manager = SchemaManager()
        self.metadata_tracker = MetadataTracker(self.context)
        
        # Component storage
        self.components = {}
        
        self.logger.info(f"TFX Pipeline initialized with data root: {data_root}")
    
    def run_example_gen(self):
        """Run ExampleGen component"""
        self.logger.info("Running ExampleGen...")
        
        try:
            example_gen = tfx.components.CsvExampleGen(input_base=self.data_root)
            self.context.run(example_gen)
            
            self.components['example_gen'] = example_gen
            self.logger.info("ExampleGen completed successfully")
            
            return example_gen
            
        except Exception as e:
            self.logger.error(f"ExampleGen failed: {str(e)}")
            raise
    
    def run_statistics_gen(self, example_gen):
        """Run StatisticsGen component"""
        self.logger.info("Running StatisticsGen...")
        
        try:
            statistics_gen = tfx.components.StatisticsGen(
                examples=example_gen.outputs['examples']
            )
            self.context.run(statistics_gen)
            
            self.components['statistics_gen'] = statistics_gen
            self.logger.info("StatisticsGen completed successfully")
            
            return statistics_gen
            
        except Exception as e:
            self.logger.error(f"StatisticsGen failed: {str(e)}")
            raise
    
    def run_schema_gen(self, statistics_gen):
        """Run SchemaGen component"""
        self.logger.info("Running SchemaGen...")
        
        try:
            schema_gen = tfx.components.SchemaGen(
                statistics=statistics_gen.outputs['statistics']
            )
            self.context.run(schema_gen)
            
            self.components['schema_gen'] = schema_gen
            self.logger.info("SchemaGen completed successfully")
            
            return schema_gen
            
        except Exception as e:
            self.logger.error(f"SchemaGen failed: {str(e)}")
            raise
    
    def create_curated_schema(self, schema_gen):
        """Create and import curated schema"""
        self.logger.info("Creating curated schema...")
        
        try:
            # Load the inferred schema
            schema_uri = schema_gen.outputs['schema']._artifacts[0].uri
            schema = tfdv.load_schema_text(os.path.join(schema_uri, 'schema.pbtxt'))
            
            # Apply custom modifications
            schema = self.schema_manager.customize_schema(schema)
            
            # Save curated schema
            updated_schema_dir = os.path.join(self.pipeline_root, 'updated_schema')
            os.makedirs(updated_schema_dir, exist_ok=True)
            
            schema_file = os.path.join(updated_schema_dir, 'schema.pbtxt')
            tfdv.write_schema_text(schema, schema_file)
            
            # Import curated schema
            user_schema_importer = tfx.components.ImportSchemaGen(schema_file=schema_file)
            self.context.run(user_schema_importer, enable_cache=False)
            
            self.components['user_schema_importer'] = user_schema_importer
            self.logger.info("Curated schema created and imported successfully")
            
            return user_schema_importer
            
        except Exception as e:
            self.logger.error(f"Schema curation failed: {str(e)}")
            raise
    
    def run_example_validator(self, statistics_gen, user_schema_importer):
        """Run ExampleValidator component"""
        self.logger.info("Running ExampleValidator...")
        
        try:
            example_validator = tfx.components.ExampleValidator(
                statistics=statistics_gen.outputs['statistics'],
                schema=user_schema_importer.outputs['schema']
            )
            self.context.run(example_validator)
            
            self.components['example_validator'] = example_validator
            self.logger.info("ExampleValidator completed successfully")
            
            return example_validator
            
        except Exception as e:
            self.logger.error(f"ExampleValidator failed: {str(e)}")
            raise
    
    def run_full_pipeline(self):
        """Run the complete TFX pipeline"""
        self.logger.info("Starting full TFX pipeline execution...")
        
        try:
            # Step 1: ExampleGen
            example_gen = self.run_example_gen()
            
            # Step 2: StatisticsGen
            statistics_gen = self.run_statistics_gen(example_gen)
            
            # Step 3: SchemaGen
            schema_gen = self.run_schema_gen(statistics_gen)
            
            # Step 4: Create curated schema
            user_schema_importer = self.create_curated_schema(schema_gen)
            
            # Step 5: ExampleValidator
            example_validator = self.run_example_validator(statistics_gen, user_schema_importer)
            
            self.logger.info("Full pipeline execution completed successfully!")
            
        except Exception as e:
            self.logger.error(f"Full pipeline execution failed: {str(e)}")
            raise
    
    def display_results(self):
        """Display pipeline results"""
        self.logger.info("Displaying pipeline results...")
        
        try:
            if 'user_schema_importer' in self.components:
                print("\n=== CURATED SCHEMA ===")
                self.context.show(self.components['user_schema_importer'].outputs['schema'])
            
            if 'example_validator' in self.components:
                print("\n=== VALIDATION RESULTS ===")
                self.context.show(self.components['example_validator'].outputs['anomalies'])
                
        except Exception as e:
            self.logger.error(f"Failed to display results: {str(e)}")
    
    def demonstrate_metadata_tracking(self):
        """Demonstrate metadata tracking capabilities"""
        self.logger.info("Demonstrating metadata tracking...")
        
        try:
            # Track artifact lineage
            lineage_info = self.metadata_tracker.track_example_anomalies_lineage()
            
            if lineage_info:
                print("\n=== METADATA TRACKING RESULTS ===")
                print(f"ExampleAnomalies Artifact ID: {lineage_info['artifact_id']}")
                print(f"Execution ID: {lineage_info['execution_id']}")
                print(f"Input Artifact IDs: {lineage_info['input_ids']}")
                
                # Display schema artifacts
                schema_info = self.metadata_tracker.get_schema_artifacts()
                print(f"\nSchema Artifacts Found: {len(schema_info)}")
                for i, schema in enumerate(schema_info):
                    print(f"  Schema {i+1}: URI={schema['uri']}, ID={schema['id']}")
            
        except Exception as e:
            self.logger.error(f"Metadata tracking demonstration failed: {str(e)}")
    
    def get_component(self, component_name):
        """Get a specific component by name"""
        return self.components.get(component_name)
    
    def cleanup(self):
        """Cleanup resources"""
        self.logger.info("Cleaning up pipeline resources...")
        # Add any cleanup logic here if needed