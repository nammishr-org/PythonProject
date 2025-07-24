# Python will connect to the CRH postgres DB for each env. Fetch the DB details from Vault.
# Better Pass the DB details for each env from the GHA CI/CD Pipeline
# Then it will find the audit tables and take its backup, zip it
# Then it will store the zip files to AWS S3 buckets
# Above python script must be called via GHA CI/CD pipeline


import os
import psycopg2
import boto3
import tempfile
import zipfile
from datetime import datetime, timezone
import sys

# CONFIGURATION
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'testdatabase',
    'user': 'testuser',
    'password': 'testpassword'
}

S3_BUCKET = 'audit-table-archives'
# S3_BUCKET = os.getenv('S3_BUCKET_NAME')
S3_REGION = 'us-east-1'  # e.g., 'us-east-1'
S3_PREFIX = os.getenv('S3_PREFIX')
# S3_PREFIX = "audit_zipped_files"


# STEP 1: Connect to PostgreSQL
def connect_postgres():
    conn = psycopg2.connect(**DB_CONFIG)
    return conn


def printme():
    print("Hello namrata")


# Get all audit tables
def get_audit_tables(conn):
    query = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type='BASE TABLE'
    AND LOWER(table_name) LIKE '%audit%';
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

    # STEP 3: Export tables to CSV and zip them


def backup_tables(conn, tables):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    zip_filename = f"audit_backup_{timestamp}.zip"
    zip_path = os.path.join(tempfile.gettempdir(), zip_filename)

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for schema, table in tables:
            filename = f"{schema}.{table}.csv"
            csv_path = os.path.join(tempfile.gettempdir(), filename)
            with open(csv_path, 'w') as f:
                with conn.cursor() as cur:
                    cur.copy_expert(f"COPY {schema}.{table} TO STDOUT WITH CSV HEADER", f)
            zipf.write(csv_path, arcname=filename)
            os.remove(csv_path)

    return zip_path


# def upload_zip_files_to_s3(local_dir, bucket_name, s3_prefix="audit_zipped_files"):
# def upload_zip_files_to_s3(local_dir, bucket_name, s3_prefix):
#     s3 = boto3.client('s3')
#     if not os.path.exists(local_dir):
#         raise ValueError(f"Directory does not exist: {local_dir}")
#     for file_name in os.listdir(local_dir):
#         if file_name.endswith(".zip"):
#             local_file_path = os.path.join(local_dir, file_name)
#             s3_key = f"{s3_prefix}/{file_name}" if s3_prefix else file_name
#
#             print(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_key}")
#             s3.upload_file(local_file_path, bucket_name, s3_key)
#             print(f"✅ Uploaded to s3://{S3_BUCKET}/{s3_prefix}")
#             os.remove(local_dir)

def upload_zip_files_to_s3(local_file_path, bucket_name, s3_prefix):
    s3 = boto3.client('s3')
    s3_key = f"{s3_prefix}{local_file_path}"
    print(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_key}")
    try:
        s3.upload_file(local_file_path, bucket_name, s3_key)
        print(f"✅ Uploaded to s3://{S3_BUCKET}/{s3_prefix}")
        return True
    except Exception as e:
        print(f"Error uploading {local_file_path} to S3: {e}")
        return False

# MAIN
def main():
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    print(f"🕒 Backup started at UTC: {now}")

    conn = connect_postgres()
    try:
        audit_tables = get_audit_tables(conn)
        if not audit_tables:
            print("⚠️ No audit tables found.")
            return

        print("🗜️ Zipping files and taking backup now...")
        zip_path = backup_tables(conn, audit_tables)
        print("🗜️ Uploading zipped files to S3 now..")
        # upload_zip_files_to_s3(zip_path)
        # timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        # zip_file = f"audit_backup_{timestamp}.zip"
        bucket_name = S3_BUCKET
        # s3_key = f'audit-backups/{zip_file}'
        upload_zip_files_to_s3(zip_path, bucket_name, S3_PREFIX)

        # local_path = sys.argv[1] if len(sys.argv) > 1 else "./zipped_files"
        # bucket = os.environ.get("S3_BUCKET_NAME", "audit-table-archives")
        # s3_folder = os.environ.get("S3_FOLDER", "audit_zipped_files")  # e.g., "zipped_backups"

        # upload_zip_files_to_s3(local_path, bucket, s3_folder)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
    # connect_postgres()
    # printme()
