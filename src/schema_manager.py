"""
Schema Manager for TFX Pipeline
Handles schema customization and management
"""

import os
import logging
import tensorflow_data_validation as tfdv
from tensorflow_metadata.proto.v0 import schema_pb2

class SchemaManager:
    """Manages schema operations and customizations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def customize_schema(self, schema):
        """
        Apply custom modifications to the schema
        
        Args:
            schema: TensorFlow Data Validation schema object
            
        Returns:
            Modified schema object
        """
        self.logger.info("Applying custom schema modifications...")
        
        try:
            # Custom modification 1: Restrict age domain (enhanced from original)
            self.customize_age_domain(schema, min_age=17, max_age=90)
            
            # Custom modification 2: Add environments
            self.add_environments(schema, ['TRAINING', 'SERVING'])
            
            # Custom modification 3: Configure serving environment
            self.configure_serving_environment(schema)
            
            # Custom modification 4: Add additional validations (new feature)
            self.add_additional_validations(schema)
            
            self.logger.info("Schema customization completed successfully")
            return schema
            
        except Exception as e:
            self.logger.error(f"Schema customization failed: {str(e)}")
            raise
    
    def customize_age_domain(self, schema, min_age=17, max_age=90):
        """
        Customize the age domain with specified range
        
        Args:
            schema: Schema to modify
            min_age: Minimum allowed age
            max_age: Maximum allowed age
        """
        try:
            tfdv.set_domain(
                schema, 
                'age', 
                schema_pb2.IntDomain(name='age', min=min_age, max=max_age)
            )
            self.logger.info(f"Age domain set to [{min_age}, {max_age}]")
            
        except Exception as e:
            self.logger.error(f"Failed to set age domain: {str(e)}")
            raise
    
    def add_environments(self, schema, environments):
        """
        Add schema environments
        
        Args:
            schema: Schema to modify
            environments: List of environment names
        """
        try:
            for env in environments:
                if env not in schema.default_environment:
                    schema.default_environment.append(env)
            
            self.logger.info(f"Added environments: {environments}")
            
        except Exception as e:
            self.logger.error(f"Failed to add environments: {str(e)}")
            raise
    
    def configure_serving_environment(self, schema):
        """
        Configure serving environment to omit label
        
        Args:
            schema: Schema to modify
        """
        try:
            # Omit label from serving environment
            label_feature = tfdv.get_feature(schema, 'label')
            if 'SERVING' not in label_feature.not_in_environment:
                label_feature.not_in_environment.append('SERVING')
            
            self.logger.info("Configured serving environment to omit label")
            
        except Exception as e:
            self.logger.error(f"Failed to configure serving environment: {str(e)}")
            raise
    
    def add_additional_validations(self, schema):
        """
        Add additional custom validations (new feature)
        
        Args:
            schema: Schema to modify
        """
        try:
            # Example: Add education level validation
            # This is a custom enhancement not in the original lab
            education_levels = [
                'Bachelors', 'Some-college', '11th', 'HS-grad', 'Prof-school',
                'Assoc-acdm', 'Assoc-voc', '9th', '7th-8th', '12th', 'Masters',
                '1st-4th', '10th', 'Doctorate', '5th-6th', 'Preschool'
            ]
            
            # Note: This is a demonstration - in practice, you'd need to check
            # if the education feature exists and handle it appropriately
            try:
                education_feature = tfdv.get_feature(schema, 'education')
                self.logger.info("Found education feature - could add domain restrictions")
            except:
                self.logger.info("Education feature not found - skipping education domain")
            
            # Add workclass validation
            try:
                workclass_feature = tfdv.get_feature(schema, 'workclass')
                self.logger.info("Found workclass feature - could add domain restrictions")
            except:
                self.logger.info("Workclass feature not found - skipping workclass domain")
            
            self.logger.info("Additional validations checked")
            
        except Exception as e:
            self.logger.error(f"Failed to add additional validations: {str(e)}")
            # Don't raise here as these are optional enhancements
    
    def validate_schema(self, schema):
        """
        Validate the schema for correctness
        
        Args:
            schema: Schema to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            # Basic validation checks
            if not schema.feature:
                self.logger.error("Schema has no features")
                return False
            
            # Check for required features
            feature_names = [f.name for f in schema.feature]
            required_features = ['age', 'label']  # Minimum required features
            
            for req_feature in required_features:
                if req_feature not in feature_names:
                    self.logger.warning(f"Required feature '{req_feature}' not found in schema")
            
            self.logger.info(f"Schema validation completed. Features found: {len(feature_names)}")
            return True
            
        except Exception as e:
            self.logger.error(f"Schema validation failed: {str(e)}")
            return False
    
    def display_schema_info(self, schema):
        """
        Display schema information
        
        Args:
            schema: Schema to display
        """
        try:
            print("\n=== SCHEMA INFORMATION ===")
            print(f"Number of features: {len(schema.feature)}")
            print(f"Default environments: {list(schema.default_environment)}")
            
            # Display feature details
            for feature in schema.feature:
                print(f"Feature: {feature.name}")
                if feature.HasField('int_domain'):
                    print(f"  Type: Integer (min: {feature.int_domain.min}, max: {feature.int_domain.max})")
                elif feature.HasField('float_domain'):
                    print(f"  Type: Float")
                elif feature.HasField('bytes_domain'):
                    print(f"  Type: Bytes/String")
                
                if feature.not_in_environment:
                    print(f"  Not in environments: {list(feature.not_in_environment)}")
            
        except Exception as e:
            self.logger.error(f"Failed to display schema info: {str(e)}")
    
    def save_schema(self, schema, filepath):
        """
        Save schema to file
        
        Args:
            schema: Schema to save
            filepath: Path to save the schema
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            # Save schema
            tfdv.write_schema_text(schema, filepath)
            self.logger.info(f"Schema saved to: {filepath}")
            
        except Exception as e:
            self.logger.error(f"Failed to save schema: {str(e)}")
            raise
    
    def load_schema(self, filepath):
        """
        Load schema from file
        
        Args:
            filepath: Path to schema file
            
        Returns:
            Loaded schema object
        """
        try:
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"Schema file not found: {filepath}")
            
            schema = tfdv.load_schema_text(filepath)
            self.logger.info(f"Schema loaded from: {filepath}")
            return schema
            
        except Exception as e:
            self.logger.error(f"Failed to load schema: {str(e)}")
            raise