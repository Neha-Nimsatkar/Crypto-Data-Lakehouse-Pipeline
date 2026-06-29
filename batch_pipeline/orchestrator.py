#Master pipeline orchestrator that executes the files in exact sequence:
#crypto_api_fetch -> bronze_validation -> bronze_to_silver -> gold validation / quality checks.



import os
import sys
import importlib.util

def run_script(script_relative_path):
    """Executes a Python file in the SAME process — shares SparkSession and env vars."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    absolute_path = os.path.normpath(os.path.join(base_dir, script_relative_path))

    print(f"Executing step: {script_relative_path}...")

    try:
        spec = importlib.util.spec_from_file_location("module", absolute_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        print(f"Finished step: {script_relative_path}\n")
    except Exception as e:
        raise RuntimeError(f"Step failed: {script_relative_path}\nReason: {str(e)}")

def run_pipeline():
    print("Starting Crypto Metric Data Lakehouse Sequencer...")
    
    # Verify environment variables are present without leaking secrets
    print(f"AWS_ACCESS_KEY_ID set: {bool(os.environ.get('AWS_ACCESS_KEY_ID'))}")
    print(f"AWS_SECRET_ACCESS_KEY set: {bool(os.environ.get('AWS_SECRET_ACCESS_KEY'))}\n")

    try:
        # Phase 1: Bronze Ingestion
        run_script("ingestion/crypto_api_fetch.py")
        
        # Phase 2: Bronze Validation
        run_script("ingestion/bronze_validation.py")
        
        # Phase 3: Silver Transformation
        run_script("Transformations/bronze_to_silver.py")
        
        # Phase 4: Silver Validation
        run_script("Transformations/silver_validation.py")
        
        # Phase 5: Gold Aggregations
        run_script("Transformations/silver_to_gold.py")
        
        # Phase 6: Gold Validations & Quality Checks
        run_script("Transformations/gold_validations.py")
        run_script("Transformations/gold_checks.py")
        
        print("Pipeline executed successfully.")
        
    except Exception as e:
        print(f"Pipeline execution failed:\n{str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
