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

DB_CONFIG = {
    'host': os.getenv("DB_HOST", "localhost"),
    'port': os.getenv("DB_PORT", 5432),
    'database': os.getenv("DB_NAME", "testdatabase"),
    'user': os.getenv("DB_USER", "testuser"),
    'password': os.getenv("DB_PASSWORD", "testpassword")
}


S3_BUCKET = os.getenv('S3_BUCKET_NAME')
S3_REGION = os.getenv('S3_REGION')
ENV = os.getenv('env')




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
    print(zip_path)

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


def upload_zip_files_to_s3(local_file_path, bucket_name):
    s3 = boto3.client('s3')
    s3_key = f"{ENV}/{local_file_path}"
    print(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_key}")
    try:
        s3.upload_file(local_file_path, bucket_name, s3_key)
        print(f"✅ Uploaded to s3://{S3_BUCKET}")
        return True
    except Exception as e:
        print(f"Error uploading {local_file_path} to S3: {e}")
        return False


# MAIN
def main():
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%d_%H%M%S")

    conn = connect_postgres()
    try:
        audit_tables = get_audit_tables(conn)
        print(f"Audit tables detected {audit_tables}")
        if not audit_tables:
            print("⚠️ No audit tables found.")
            return

        print("🗜️ Zipping files and taking backup now...")
        zip_path = backup_tables(conn, audit_tables)
        print("🗜️ Uploading zipped files to S3 now..")
        upload_zip_files_to_s3(zip_path, S3_BUCKET)
    finally:
        conn.close()


if __name__ == "__main__":
    main()