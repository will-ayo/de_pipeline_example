import requests
import json
import logging
from google.cloud import storage
from typing import Dict, Any
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_data(url: str) -> Dict[str, Any]:
    """Fetch data from the FDA API."""
    try:
        logger.info(f"Requesting data from {url}")
        session = requests.Session()
        retries = requests.adapters.Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retries)
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        
        # Log connection attempt
        logger.info("Initiating request with timeout settings: connect=10s, read=60s")
        
        response = session.get(url, timeout=(10, 60), stream=True)
        
        # Log response headers and status
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response headers: {dict(response.headers)}")
        
        response.raise_for_status()
        
        # Track chunked reading progress
        content = b''
        chunks_received = 0
        total_bytes = 0
        
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                chunks_received += 1
                total_bytes += len(chunk)
                content += chunk
                if chunks_received % 100 == 0:  # Log every 100 chunks
                    logger.info(f"Download progress: {total_bytes/1024/1024:.2f} MB ({chunks_received} chunks)")
        
        logger.info(f"Download completed: {total_bytes/1024/1024:.2f} MB total")
        return json.loads(content)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response status code: {e.response.status_code}")
            logger.error(f"Response headers: {dict(e.response.headers)}")
        raise
    finally:
        session.close()

def upload_to_gcs(bucket_name: str, destination_blob: str, data: Dict[str, Any]) -> None:
    """Upload data directly to Google Cloud Storage."""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)
        
        # Convert dict to JSON string and encode to bytes
        json_data = json.dumps(data, indent=4)
        
        # Upload directly from memory
        blob.upload_from_string(json_data, content_type='application/json')
        logger.info(f"Uploaded data to gs://{bucket_name}/{destination_blob}")
    except Exception as e:
        logger.error(f"Error uploading to GCS: {e}")
        raise

def validate_data(data: Dict[str, Any]) -> bool:
    """Basic data validation."""
    required_keys = ['results', 'meta']
    return all(key in data for key in required_keys)

def main():
    try:
        # Generate timestamp for filename
        filename = f"drug_events_{Config.get_timestamp()}.json"
        
        # Fetch and validate data
        data = fetch_data(Config.API_URL)
        if not validate_data(data):
            raise ValueError("Data validation failed: missing required fields")
        
        # Upload directly to GCS
        gcs_path = f"{Config.INGESTION_PATH}/{filename}"
        upload_to_gcs(Config.BUCKET_NAME, gcs_path, data)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
