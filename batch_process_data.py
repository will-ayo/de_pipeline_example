import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from typing import Dict, Any
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DrugEventTransforms(beam.DoFn):
    """Transform drug event records."""
    
    def process(self, element: Dict[str, Any]):
        """Process individual drug event records."""
        try:
            if not element or 'results' not in element:
                logger.warning("Invalid record structure")
                return

            # Process each result in the batch
            for result in element['results']:
                if not isinstance(result, dict):
                    continue

                # Transform the record
                transformed = {
                    'report_id': result.get('safetyreportid'),
                    'receive_date': result.get('receivedate'),
                    'serious': result.get('serious'),
                    'patient_age': self._normalize_age(result.get('patient', {})),
                    'processed': True,
                    'processing_timestamp': beam.window.TimestampedValue.get_current_timestamp().to_utc_datetime().isoformat()
                }

                if transformed['report_id']:  # Only yield if we have a valid report ID
                    yield transformed

        except Exception as e:
            logger.error(f"Error transforming record: {e}")
            return

    def _normalize_age(self, patient: Dict[str, Any]) -> float:
        """Normalize patient age to years."""
        try:
            if 'patientonsetage' not in patient:
                return None

            age = float(patient['patientonsetage'])
            age_unit = patient.get('patientonsetageunit', 'year')

            if age_unit == 'month':
                return age / 12
            elif age_unit == 'day':
                return age / 365
            return age

        except (ValueError, TypeError):
            return None

def run_pipeline():
    """Execute the Apache Beam pipeline."""
    pipeline_options = PipelineOptions(
        project=Config.PROJECT,
        job_name='drug-events-batch-processing',
        temp_location=f"gs://{Config.BUCKET_NAME}/{Config.TEMP_PATH}",
        streaming=False,
        save_main_session=True,
        runner='DirectRunner'
    )

    try:
        with beam.Pipeline(options=pipeline_options) as p:
            # Read and process data
            processed_data = (
                p
                | 'Read JSON Files' >> beam.io.ReadFromText(Config.get_input_path())
                | 'Parse JSON' >> beam.Map(json.loads)
                | 'Transform Records' >> beam.ParDo(DrugEventTransforms())
            )

            # Write successful records
            (processed_data 
                | 'Filter Successful' >> beam.Filter(lambda x: x.get('processed', False))
                | 'Format Successful JSON' >> beam.Map(json.dumps)
                | 'Write Successful' >> beam.io.WriteToText(
                    Config.get_output_path(),
                    file_name_suffix=".json",
                    num_shards=Config.NUM_SHARDS
                )
            )

            # Write failed records
            (processed_data
                | 'Filter Failed' >> beam.Filter(lambda x: not x.get('processed', False))
                | 'Format Failed JSON' >> beam.Map(json.dumps)
                | 'Write Failed' >> beam.io.WriteToText(
                    f"gs://{Config.BUCKET_NAME}/{Config.FAILED_PATH}",
                    file_name_suffix=".json"
                )
            )

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()
