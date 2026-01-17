import functions_framework
import json
import os
from datetime import datetime
import duckdb
import traceback
from kafka_logger import get_kafka_logger, log_event


def convert_csv_to_parquet_gcs(csv_uri, output_uri):
    """
    Convert CSV to Parquet directly in GCS using DuckDB.
    No download/upload needed - DuckDB handles it all!
    """
    try:
        log_event('csv_conversion_started', {
            'csv_uri': csv_uri,
            'output_uri': output_uri
        })

        # DuckDB with GCS extension
        conn = duckdb.connect(':memory:')

        # Install and load httpfs extension for GCS access
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")

        # Get HMAC credentials from environment
        gcs_key_id = os.environ.get('GCS_HMAC_KEY')
        gcs_secret = os.environ.get('GCS_HMAC_SECRET')

        if not gcs_key_id or not gcs_secret:
            raise ValueError("GCS_HMAC_KEY and GCS_HMAC_SECRET environment variables must be set")

        # Configure GCS access with HMAC credentials
        conn.execute(f"""
            CREATE SECRET (
                TYPE gcs,
                KEY_ID '{gcs_key_id}',
                SECRET '{gcs_secret}'
            );
        """)

        conn.execute("SET enable_http_metadata_cache=true;")

        # Get stats before conversion
        row_count_result = conn.execute(f"""
            SELECT COUNT(*) FROM read_csv_auto('{csv_uri}')
        """).fetchone()
        row_count = row_count_result[0] if row_count_result else 0

        # Get column info
        columns_info = conn.execute(f"""
            DESCRIBE SELECT * FROM read_csv_auto('{csv_uri}')
        """).fetchall()

        column_names = [col[0] for col in columns_info]
        column_types = {col[0]: col[1] for col in columns_info}

        log_event('csv_parsed', {
            'csv_uri': csv_uri,
            'rows': row_count,
            'columns': len(column_names),
            'column_names': column_names,
            'dtypes': column_types
        })

        # Convert CSV to Parquet directly in GCS!
        conn.execute(f"""
            COPY (SELECT * FROM read_csv_auto('{csv_uri}'))
            TO '{output_uri}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)

        conn.close()

        log_event('csv_conversion_completed', {
            'csv_uri': csv_uri,
            'output_uri': output_uri,
            'rows': row_count,
            'columns': len(column_names)
        })

        return {
            'rows': row_count,
            'columns': len(column_names),
            'column_names': column_names
        }

    except Exception as e:
        log_event('csv_conversion_failed', {
            'csv_uri': csv_uri,
            'output_uri': output_uri,
            'error': str(e),
            'traceback': traceback.format_exc()
        })
        raise


@functions_framework.http
def csv_to_parquet(request):
    """
    HTTP Cloud Function to convert CSV to Parquet in GCS using DuckDB.
    DuckDB reads from GCS and writes to GCS directly - no download/upload needed!

    Accepts JSON with 'csv_uri' pointing to GCS file (gs://bucket/path/file.csv)

    Returns JSON with GCS URI of the Parquet file and processing stats.
    """
    request_id = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')

    try:
        log_event('request_received', {
            'request_id': request_id,
            'method': request.method,
            'content_type': request.content_type
        })

        # Get configuration from environment
        output_bucket = os.environ.get('GCS_OUTPUT_BUCKET')
        if not output_bucket:
            raise ValueError("GCS_OUTPUT_BUCKET environment variable not set")

        # Only accept POST with JSON
        if request.method != 'POST':
            raise ValueError("Only POST method is supported")

        if not request.is_json:
            raise ValueError("Request must be JSON with 'csv_uri'")

        request_json = request.get_json()

        if 'csv_uri' not in request_json:
            raise ValueError("JSON must contain 'csv_uri' field")

        csv_uri = request_json['csv_uri']

        # Validate GCS URI
        if not csv_uri.startswith('gs://'):
            raise ValueError("csv_uri must start with gs://")

        # Parse input URI
        csv_filename = csv_uri.split('/')[-1]
        base_filename = csv_filename.rsplit('.', 1)[0] if csv_filename else f'converted_{request_id}'

        # Generate output URI
        output_path = os.environ.get('GCS_OUTPUT_PATH', 'parquet/')
        output_uri = f"gs://{output_bucket}/{output_path}{base_filename}_{request_id}.parquet"

        log_event('conversion_request', {
            'request_id': request_id,
            'csv_uri': csv_uri,
            'output_uri': output_uri
        })

        # Convert CSV to Parquet directly in GCS using DuckDB!
        convert_csv_to_parquet_gcs(csv_uri, output_uri)

        # Flush Kafka producer
        get_kafka_logger().flush(timeout=5)

        # Success response
        response = {
            'status': 'success',
            'event': 'csv_to_parquet_completed'
        }

        log_event('request_completed', {
            'request_id': request_id,
            'response': response
        })

        return json.dumps(response), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        error_msg = str(e)
        error_trace = traceback.format_exc()

        log_event('request_failed', {
            'request_id': request_id,
            'error': error_msg,
            'traceback': error_trace
        })

        # Try to flush Kafka messages even on error
        try:
            get_kafka_logger().flush(timeout=2)
        except:
            pass

        error_response = {
            'status': 'error',
            'request_id': request_id,
            'error': error_msg
        }

        return json.dumps(error_response), 500, {'Content-Type': 'application/json'}
