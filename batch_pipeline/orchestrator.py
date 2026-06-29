# master pipeline orchestrator — runs all steps in sequence:
# crypto_api_fetch -> bronze_validation -> bronze_to_silver -> silver_validation -> silver_to_gold -> gold_validations



import os
import sys
import subprocess

def run_script_via_cli(script_relative_path):
    """runs a python file in the current compute context"""
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    absolute_path = os.path.normpath(os.path.join(base_dir, script_relative_path))
    
    print(f"running: {script_relative_path}...")
    
    result = subprocess.run([sys.executable, absolute_path], capture_output=False, text=True)
    
    if result.returncode != 0:
        raise RuntimeError(f"step failed — exit code {result.returncode}: {script_relative_path}")
    print(f"done: {script_relative_path}\n")

def run_pipeline():
    print("starting crypto lakehouse pipeline...")
    
    try:
        # step 1 — ingest from CoinGecko API to bronze
        print("\nstep 1 — bronze ingestion")
        run_script_via_cli("ingestion/crypto_api_fetch.py")
        
        # step 2 — validate bronze data
        print("\nstep 2 — bronze validation")
        run_script_via_cli("ingestion/bronze_validation.py")
        
        # step 3 — transform bronze to silver
        print("\nstep 3 — bronze to silver")
        # note: capital T matches actual folder name on disk
        run_script_via_cli("Transformations/bronze_to_silver.py")
        
        # step 4 — validate silver data
        print("\nstep 4 — silver validation")
        run_script_via_cli("Transformations/silver_validation.py")

        # step 5 — aggregate silver to gold
        print("\nstep 5 — silver to gold")
        run_script_via_cli("Transformations/silver_to_gold.py")

        # step 6 — validate gold tables
        print("\nstep 6 — gold validation")
        run_script_via_cli("Transformations/gold_validations.py")
        run_script_via_cli("Transformations/gold_checks.py")
        
        print("pipeline completed successfully")

    except Exception as e:
        print(f"\npipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()