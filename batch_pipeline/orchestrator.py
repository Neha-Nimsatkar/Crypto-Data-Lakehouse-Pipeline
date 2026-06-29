#Master pipeline orchestrator that executes the files in exact sequence:
#crypto_api_fetch -> bronze_validation -> bronze_to_silver -> gold validation / quality checks.



import os
import sys
import subprocess

def run_script_via_cli(script_relative_path):
    """Executes a Python file cleanly in the current Databricks compute context."""
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    absolute_path = os.path.normpath(os.path.join(base_dir, script_relative_path))
    
    print(f" Executing step: {script_relative_path}...")
    
  
    result = subprocess.run([sys.executable, absolute_path], capture_output=False, text=True)
    
    if result.returncode != 0:
        raise RuntimeError(f" Step failed with exit code {result.returncode}: {script_relative_path}")
    print(f" Finished step: {script_relative_path} safely.\n")

def run_pipeline():
    print("==================================================================")
    print(" STARTING CRYPTO METRIC DATA LAKEHOUSE METRIC SEQUENCER")
    print("==================================================================")
    
    try:
        # ── STEP 1: BRONZE INGESTION LAYER ─────────────────────────────────────
        print("\n [PHASE 1] RUNNING INGESTION PROCESS...")
        run_script_via_cli("ingestion/crypto_api_fetch.py")
        
        # ── STEP 2: BRONZE QUALITY DATA VALIDATION ────────────────────────────
        print("\n [PHASE 2] INITIATING BRONZE METRIC DATA VALIDATIONS...")
        run_script_via_cli("ingestion/bronze_validation.py")
        
        # ── STEP 3: SILVER LAYER ETL TRANSFORMATION ────────────────────────────
        print("\n [PHASE 3] RUNNING BRONZE TO SILVER DELTA LAKE TRANSFORMATIONS...")
        # Note: Capital 'T' as referenced from the directory setup in image_80db1f.png
        run_script_via_cli("Transformations/bronze_to_silver.py")
        
        # ── STEP 4: SILVER LAYER VALIDATION ────────────────────────────────────
        print("\n [PHASE 4] EXECUTION OF SILVER TRANSFORMATION INTEGRITY CHECKS...")
        run_script_via_cli("Transformations/silver_validation.py")

        # ── STEP 5: GOLD LAYER AGGREGATIONS ────────────────────────────────────
        print("\n [PHASE 5] PROCESSING SILVER TO GOLD VALUE AGGREGATIONS...")
        run_script_via_cli("Transformations/silver_to_gold.py")

        # ── STEP 6: GOLD LAYER VALIDATIONS & FINAL SQL QUALITY CHECKS ──────────
        print("\n [PHASE 6] RUNNING TARGET ANALYTICAL AND SQL QUALITY VALIDATIONS...")
        run_script_via_cli("Transformations/gold_validations.py")
        run_script_via_cli("Transformations/gold_checks.py")
        
        print("==================================================================")
        print(" END-TO-END MEDALLION DATA SYSTEM SYNCHRONIZED SUCCESSFULLY!")
       

    except Exception as e:
        print("\n PIPELINE EXECUTION CRASHED IN SEQUENCER CRITICAL NODE:")
        print(str(e))
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
 