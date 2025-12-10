"""
Metadata Tracker for TFX Pipeline
Handles ML Metadata exploration and artifact lineage tracking
"""

import logging
import ml_metadata as mlmd
from ml_metadata.proto import metadata_store_pb2

class MetadataTracker:
    """Tracks and explores ML Metadata"""
    
    def __init__(self, context):
        """
        Initialize metadata tracker
        
        Args:
            context: TFX InteractiveContext instance
        """
        self.context = context
        self.logger = logging.getLogger(__name__)
        
        # Setup metadata store connection
        try:
            connection_config = context.metadata_connection_config
            self.store = mlmd.MetadataStore(connection_config)
            self.logger.info("Metadata store connection established")
        except Exception as e:
            self.logger.error(f"Failed to connect to metadata store: {str(e)}")
            raise
    
    def get_artifact_types(self):
        """
        Get all artifact types from metadata store
        
        Returns:
            List of artifact type names
        """
        try:
            artifact_types = self.store.get_artifact_types()
            type_names = [artifact_type.name for artifact_type in artifact_types]
            
            self.logger.info(f"Found {len(type_names)} artifact types")
            return type_names
            
        except Exception as e:
            self.logger.error(f"Failed to get artifact types: {str(e)}")
            return []
    
    def get_schema_artifacts(self):
        """
        Get all Schema artifacts from metadata store
        
        Returns:
            List of schema artifact information
        """
        try:
            schema_list = self.store.get_artifacts_by_type('Schema')
            schema_info = [
                {'uri': schema.uri, 'id': schema.id} 
                for schema in schema_list
            ]
            
            self.logger.info(f"Found {len(schema_info)} Schema artifacts")
            return schema_info
            
        except Exception as e:
            self.logger.error(f"Failed to get schema artifacts: {str(e)}")
            return []
    
    def get_example_anomalies_artifacts(self):
        """
        Get all ExampleAnomalies artifacts from metadata store
        
        Returns:
            List of ExampleAnomalies artifacts
        """
        try:
            anomalies_list = self.store.get_artifacts_by_type('ExampleAnomalies')
            self.logger.info(f"Found {len(anomalies_list)} ExampleAnomalies artifacts")
            return anomalies_list
            
        except Exception as e:
            self.logger.error(f"Failed to get ExampleAnomalies artifacts: {str(e)}")
            return []
    
    def track_artifact_lineage(self, artifact_id):
        """
        Track the lineage of a specific artifact
        
        Args:
            artifact_id: ID of the artifact to track
            
        Returns:
            Dictionary containing lineage information
        """
        try:
            # Get events for the artifact
            events = self.store.get_events_by_artifact_ids([artifact_id])
            
            if not events:
                self.logger.warning(f"No events found for artifact ID: {artifact_id}")
                return None
            
            # Get the first event (should be OUTPUT event)
            first_event = events[0]
            execution_id = first_event.execution_id
            
            # Get all events for this execution
            execution_events = self.store.get_events_by_execution_ids([execution_id])
            
            # Separate inputs and outputs
            input_artifacts = [
                event.artifact_id for event in execution_events 
                if event.type == metadata_store_pb2.Event.INPUT
            ]
            
            output_artifacts = [
                event.artifact_id for event in execution_events 
                if event.type == metadata_store_pb2.Event.OUTPUT
            ]
            
            lineage_info = {
                'artifact_id': artifact_id,
                'execution_id': execution_id,
                'input_ids': input_artifacts,
                'output_ids': output_artifacts
            }
            
            self.logger.info(f"Tracked lineage for artifact {artifact_id}")
            return lineage_info
            
        except Exception as e:
            self.logger.error(f"Failed to track artifact lineage: {str(e)}")
            return None
    
    def track_example_anomalies_lineage(self):
        """
        Track the lineage of ExampleAnomalies artifacts
        
        Returns:
            Lineage information for the first ExampleAnomalies artifact
        """
        try:
            # Get ExampleAnomalies artifacts
            anomalies_artifacts = self.get_example_anomalies_artifacts()
            
            if not anomalies_artifacts:
                self.logger.warning("No ExampleAnomalies artifacts found")
                return None
            
            # Track the first one
            first_anomalies = anomalies_artifacts[0]
            lineage_info = self.track_artifact_lineage(first_anomalies.id)
            
            return lineage_info
            
        except Exception as e:
            self.logger.error(f"Failed to track ExampleAnomalies lineage: {str(e)}")
            return None
    
    def get_execution_info(self, execution_id):
        """
        Get detailed information about an execution
        
        Args:
            execution_id: ID of the execution
            
        Returns:
            Dictionary containing execution information
        """
        try:
            executions = self.store.get_executions_by_id([execution_id])
            
            if not executions:
                self.logger.warning(f"No execution found with ID: {execution_id}")
                return None
            
            execution = executions[0]
            
            execution_info = {
                'id': execution.id,
                'type_id': execution.type_id,
                'last_known_state': execution.last_known_state,
                'properties': dict(execution.properties) if execution.properties else {}
            }
            
            self.logger.info(f"Retrieved execution info for ID: {execution_id}")
            return execution_info
            
        except Exception as e:
            self.logger.error(f"Failed to get execution info: {str(e)}")
            return None
    
    def display_artifact_summary(self):
        """Display a summary of all artifacts in the metadata store"""
        try:
            print("\n=== METADATA STORE SUMMARY ===")
            
            # Get artifact types
            artifact_types = self.get_artifact_types()
            print(f"Artifact Types: {artifact_types}")
            
            # Count artifacts by type
            for artifact_type in artifact_types:
                try:
                    artifacts = self.store.get_artifacts_by_type(artifact_type)
                    print(f"  {artifact_type}: {len(artifacts)} artifacts")
                except:
                    print(f"  {artifact_type}: Unable to count")
            
        except Exception as e:
            self.logger.error(f"Failed to display artifact summary: {str(e)}")
    
    def display_lineage_graph(self, artifact_id):
        """
        Display a simple text-based lineage graph
        
        Args:
            artifact_id: ID of the artifact to trace
        """
        try:
            lineage = self.track_artifact_lineage(artifact_id)
            
            if not lineage:
                print(f"No lineage information found for artifact {artifact_id}")
                return
            
            print(f"\n=== LINEAGE GRAPH FOR ARTIFACT {artifact_id} ===")
            print(f"Execution ID: {lineage['execution_id']}")
            print("Inputs:")
            for input_id in lineage['input_ids']:
                print(f"  └── Artifact {input_id}")
            print("Outputs:")
            for output_id in lineage['output_ids']:
                print(f"  └── Artifact {output_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to display lineage graph: {str(e)}")
    
    def find_artifacts_by_uri_pattern(self, pattern):
        """
        Find artifacts by URI pattern
        
        Args:
            pattern: Pattern to match in URI
            
        Returns:
            List of matching artifacts
        """
        try:
            all_artifacts = []
            artifact_types = self.get_artifact_types()
            
            for artifact_type in artifact_types:
                try:
                    artifacts = self.store.get_artifacts_by_type(artifact_type)
                    for artifact in artifacts:
                        if pattern in artifact.uri:
                            all_artifacts.append({
                                'id': artifact.id,
                                'type': artifact_type,
                                'uri': artifact.uri
                            })
                except:
                    continue
            
            self.logger.info(f"Found {len(all_artifacts)} artifacts matching pattern '{pattern}'")
            return all_artifacts
            
        except Exception as e:
            self.logger.error(f"Failed to find artifacts by URI pattern: {str(e)}")
            return []