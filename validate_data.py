# data_quality.py
import pandas as pd
import great_expectations as ge
import json
import logging
from typing import Dict, Any, List
from config import Config
from google.cloud import storage
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataValidator:
    """Handles data validation using Great Expectations."""
    
    def __init__(self):
        self.expectations = [
            # Core data quality checks
            {"name": "expect_column_to_exist", "args": ["safetyreportid"]},
            {"name": "expect_column_values_to_not_be_null", "args": ["safetyreportid"]},
            {"name": "expect_column_values_to_be_unique", "args": ["safetyreportid"]},
            
            # Date validation
            {"name": "expect_column_to_exist", "args": ["receivedate"]},
            {"name": "expect_column_values_to_not_be_null", "args": ["receivedate"]},
            
            # Patient data validation
            {"name": "expect_column_to_exist", "args": ["patient.patientonsetage"]},
            {"name": "expect_column_values_to_be_between", 
             "args": ["patient.patientonsetage"], 
             "kwargs": {"min_value": 0, "max_value": 120}}
        ]

    def validate_file(self, file_path: str) -> Dict[str, Any]:
        """Validate a single JSON file."""
        try:
            # Load and prepare data
            with open(file_path, "r") as f:
                data = json.load(f)
            
            df = pd.DataFrame(data.get('results', []))
            if df.empty:
                logger.warning(f"No data found in {file_path}")
                return {"success": False, "message": "No data found"}

            # Convert to Great Expectations DataFrame
            ge_df = ge.from_pandas(df)
            
            # Apply all expectations
            results = []
            for exp in self.expectations:
                name = exp["name"]
                args = exp.get("args", [])
                kwargs = exp.get("kwargs", {})
                
                try:
                    result = getattr(ge_df, name)(*args, **kwargs)
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error applying expectation {name}: {str(e)}")
                    results.append({"success": False, "expectation": name, "error": str(e)})

            return {
                "filename": file_path,
                "timestamp": Config.get_timestamp(),
                "record_count": len(df),
                "validation_results": results
            }

        except Exception as e:
            logger.error(f"Error validating file {file_path}: {str(e)}")
            return {"success": False, "error": str(e)}

    def save_results(self, results: Dict[str, Any], output_path: str):
        """Save validation results to file."""
        try:
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2)
            logger.info(f"Validation results saved to {output_path}")
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")
            raise

def main():
    """Main execution function."""
    try:
        # Initialize validator
        validator = DataValidator()
        
        # Create data directory if it doesn't exist
        os.makedirs(Config.LOCAL_DATA_DIR, exist_ok=True)
        
        # Validate each file in the data directory
        input_dir = os.path.join(Config.LOCAL_DATA_DIR, Config.INGESTION_PATH)
        for filename in os.listdir(input_dir):
            if filename.endswith('.json'):
                file_path = os.path.join(input_dir, filename)
                
                # Run validation
                logger.info(f"Validating file: {filename}")
                results = validator.validate_file(file_path)
                
                # Save results
                output_filename = f"validation_{Config.get_timestamp()}_{filename}"
                output_path = os.path.join(Config.LOCAL_DATA_DIR, 'validation', output_filename)
                validator.save_results(results, output_path)
                
                # Log summary
                success_count = sum(1 for r in results.get('validation_results', []) 
                                  if r.get('success', False))
                logger.info(f"Validation complete: {success_count}/{len(results.get('validation_results', []))} "
                          f"expectations passed")

    except Exception as e:
        logger.error(f"Validation pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
