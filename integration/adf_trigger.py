"""
Azure Data Factory Integration
Trigger and monitor ADF pipelines programmatically
"""

from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ADFPipelineManager:
    """Manage Azure Data Factory pipelines"""
    
    def __init__(self, subscription_id, resource_group, factory_name):
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.factory_name = factory_name
        
        # Authenticate
        self.credential = DefaultAzureCredential()
        self.adf_client = DataFactoryManagementClient(
            self.credential,
            self.subscription_id
        )
    
    def trigger_pipeline(self, pipeline_name, parameters=None):
        """Trigger a pipeline run"""
        logger.info(f"Triggering pipeline: {pipeline_name}")
        
        try:
            run_response = self.adf_client.pipelines.create_run(
                self.resource_group,
                self.factory_name,
                pipeline_name,
                parameters=parameters or {}
            )
            
            run_id = run_response.run_id
            logger.info(f"Pipeline run started with ID: {run_id}")
            
            return run_id
            
        except Exception as e:
            logger.error(f"Failed to trigger pipeline: {str(e)}")
            raise
    
    def get_pipeline_run_status(self, run_id):
        """Get pipeline run status"""
        try:
            run_status = self.adf_client.pipeline_runs.get(
                self.resource_group,
                self.factory_name,
                run_id
            )
            
            return {
                'run_id': run_status.run_id,
                'pipeline_name': run_status.pipeline_name,
                'status': run_status.status,
                'start_time': run_status.run_start.isoformat() if run_status.run_start else None,
                'end_time': run_status.run_end.isoformat() if run_status.run_end else None,
                'duration': run_status.duration_in_ms,
                'message': run_status.message
            }
            
        except Exception as e:
            logger.error(f"Failed to get run status: {str(e)}")
            raise
    
    def wait_for_pipeline_completion(self, run_id, timeout=3600, poll_interval=30):
        """Wait for pipeline to complete"""
        logger.info(f"Waiting for pipeline run {run_id} to complete...")
        
        start_time = time.time()
        
        while True:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Pipeline run {run_id} exceeded timeout of {timeout} seconds")
            
            status_info = self.get_pipeline_run_status(run_id)
            status = status_info['status']
            
            logger.info(f"Current status: {status}")
            
            if status in ['Succeeded', 'Failed', 'Cancelled']:
                return status_info
            
            time.sleep(poll_interval)
    
    def list_pipeline_runs(self, last_updated_after=None, last_updated_before=None):
        """List recent pipeline runs"""
        try:
            from datetime import datetime, timedelta
            
            if not last_updated_after:
                last_updated_after = datetime.now() - timedelta(days=1)
            if not last_updated_before:
                last_updated_before = datetime.now()
            
            filter_params = RunFilterParameters(
                last_updated_after=last_updated_after,
                last_updated_before=last_updated_before
            )
            
            runs = self.adf_client.pipeline_runs.query_by_factory(
                self.resource_group,
                self.factory_name,
                filter_params
            )
            
            return [
                {
                    'run_id': run.run_id,
                    'pipeline_name': run.pipeline_name,
                    'status': run.status,
                    'start_time': run.run_start.isoformat() if run.run_start else None,
                    'end_time': run.run_end.isoformat() if run.run_end else None
                }
                for run in runs.value
            ]
            
        except Exception as e:
            logger.error(f"Failed to list pipeline runs: {str(e)}")
            raise


def main():
    """Example usage"""
    
    # Configuration
    subscription_id = "your-subscription-id"
    resource_group = "rg-enterprise-dw"
    factory_name = "adf-enterprise-dw"
    pipeline_name = "PL_FullLoad_OnPremToAzureDW"
    
    # Initialize manager
    manager = ADFPipelineManager(subscription_id, resource_group, factory_name)
    
    # Trigger pipeline
    parameters = {
        "LastLoadDate": "2024-01-01"
    }
    
    run_id = manager.trigger_pipeline(pipeline_name, parameters)
    
    # Wait for completion
    result = manager.wait_for_pipeline_completion(run_id)
    
    logger.info(f"Pipeline completed with status: {result['status']}")
    
    # List recent runs
    recent_runs = manager.list_pipeline_runs()
    logger.info(f"Found {len(recent_runs)} recent pipeline runs")


if __name__ == "__main__":
    main()
