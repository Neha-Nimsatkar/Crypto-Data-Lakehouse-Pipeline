import boto3
import os
from botocore.exceptions import ClientError

# Configuration
BUCKET_NAME = 'crypto-lakehouse-neha'
LOCAL_BRONZE_DIR = 'data/bronze'

# Initialize S3 Client
s3 = boto3.client('s3')

def file_exists_in_s3(bucket, s3_path):
    """Checks if a file already exists in the S3 bucket."""
    try:
        s3.head_object(Bucket=bucket, Key=s3_path)
        return True
    except ClientError:
        return False

def upload_to_s3():
    print(f"--- Checking & Uploading to S3: {BUCKET_NAME} ---")
    
    # Filter for json files
    files = [f for f in os.listdir(LOCAL_BRONZE_DIR) if f.endswith('.json')]
    
    if not files:
        print("No local files found in data/bronze.")
        return

    uploaded_count = 0
    skipped_count = 0

    for filename in files:
        local_path = os.path.join(LOCAL_BRONZE_DIR, filename)
        s3_path = f"bronze/{filename}"
        
        # Avoid duplicate uploads
        if file_exists_in_s3(BUCKET_NAME, s3_path):
            skipped_count += 1
            continue

        try:
            s3.upload_file(local_path, BUCKET_NAME, s3_path)
            print(f" [UPLOADED]: {filename}")
            uploaded_count += 1
        except Exception as e:
            print(f" [FAILED]: {filename} | Error: {e}")

    print(f"\n--- Summary ---")
    print(f"Uploaded: {uploaded_count}")
    print(f"Skipped (Already in S3): {skipped_count}")

if __name__ == "__main__":
    upload_to_s3()
