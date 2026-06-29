import os
import sys
import subprocess

def run_script_via_cli(script_relative_path, aws_credentials):
    """Executes a Python file in a clean subprocess, passing AWS credentials forward."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    absolute_path = os.path.normpath(os.path.join(base_dir, script_relative_path))
    
    print(f"Executing step: {script_relative_path}...")
    
    # Clone the existing environment and inject the dynamic AWS secrets
    current_env = os.environ.copy()
    current_env.update(aws_credentials)
    
    # Execute the script passing down the environment keys
    result = subprocess.run(
        [sys.executable, absolute_path], 
        capture_output=False, 
        text=True,
        env=current_env
    )
    
    if result.returncode != 0:
        raise RuntimeError(f"Step failed with exit code {result.returncode}: {script_relative_path}")
        
    print(f"Finished step: {script_relative_path} safely.\n")

def run_pipeline():
    print("Starting Crypto Metric Data Lakehouse Sequencer on Serverless Compute...")
    
    # 1. Safely fetch secrets from Databricks Secrets utility
    aws_secrets = {}
    try:
        aws_secrets["AWS_ACCESS_KEY_ID"] = dbutils.secrets.get(scope="crypto-pipeline-secrets", key="aws_id")
        aws_secrets["AWS_SECRET_ACCESS_KEY"] = dbutils.secrets.get(scope="crypto-pipeline-secrets", key="aws_secret")
        print("AWS credentials safely initialized from Databricks Secrets.\n")
    except Exception as e:
        print(f"Warning: Failed to fetch secrets from dbutils (relying on environment): {str(e)}\n")

    try:
        # Phase 1: Bronze Ingestion
        run_script_via_cli("ingestion/crypto_api_fetch.py", aws_secrets)
        
        # Phase 2: Bronze Validation
        run_script_via_cli("ingestion/bronze_validation.py", aws_secrets)
        
        # Phase 3: Silver Transformation
        run_script_via_cli("Transformations/bronze_to_silver.py", aws_secrets)
        
        # Phase 4: Silver Validation
        run_script_via_cli("Transformations/silver_validation.py", aws_secrets)

        # Phase 5: Gold Aggregations
        run_script_via_cli("Transformations/silver_to_gold.py", aws_secrets)

        # Phase 6: Gold Validations & Quality Checks
        run_script_via_cli("Transformations/gold_validations.py", aws_secrets)
        run_script_via_cli("Transformations/gold_checks.py", aws_secrets)
        
        print("End-to-End Medallion Data System Synchronized Successfully!")

    except Exception as e:
        print(f"\nPipeline execution crashed:\n{str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()