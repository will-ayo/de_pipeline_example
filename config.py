import os
from datetime import datetime

class Config:
    # GCP Settings
    PROJECT = os.getenv('GCP_PROJECT', 'rare-hub-450814-d2')
    BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'drug-events')
    
    # API Settings
    API_URL = 'https://api.fda.gov/drug/event.json?limit=100'
    
    # Storage paths
    LOCAL_DATA_DIR = 'data'
    INGESTION_PATH = 'ingestion'
    PROCESSED_PATH = 'processed/drug_events'
    FAILED_PATH = 'failed/failed_records'
    TEMP_PATH = 'temp'
    
    # Batch processing settings
    NUM_SHARDS = 5
    
    @classmethod
    def get_timestamp(cls) -> str:
        """Get current timestamp string."""
        return datetime.now().strftime('%Y%m%d_%H%M%S')
    
    @classmethod
    def get_input_path(cls) -> str:
        """Get GCS input path pattern."""
        return f"gs://{cls.BUCKET_NAME}/{cls.INGESTION_PATH}/*.json"
    
    @classmethod
    def get_output_path(cls) -> str:
        """Get GCS output path."""
        return f"gs://{cls.BUCKET_NAME}/{cls.PROCESSED_PATH}" 