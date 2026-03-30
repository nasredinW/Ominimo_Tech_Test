
"""
Dynamic Spark Pipeline DAG - S3 Integration

Workflow:
  1. Extract metadata config from S3
  2. Read source data from S3
  3. Apply transformations based on metadata config
  4. Write output to S3
"""

from datetime import timedelta
from pathlib import Path
import json
import logging
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Configure logging
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
SRC_PATH = PROJECT_ROOT / "src"
METADATA_PATH = PROJECT_ROOT / "metadata"
DATA_PATH = PROJECT_ROOT / "data"
LOCAL_CONFIG_PATH = DATA_PATH / "local_config"

# Add src to path for imports
sys.path.insert(0, str(SRC_PATH))

# Create local config directory (with fallback for permission issues)
try:
    LOCAL_CONFIG_PATH.mkdir(parents=True, exist_ok=True)
except PermissionError:
    logger.warning(f"Unable to create {LOCAL_CONFIG_PATH} - directory may not be writable. This is OK if using S3.")
except Exception as e:
    logger.warning(f"Error creating {LOCAL_CONFIG_PATH}: {e}")

# =========================================================================
# AIRFLOW VARIABLES CONFIGURATION
# =========================================================================

# S3 Configuration
S3_BUCKET = Variable.get('s3_bucket', 'data-pipeline')
S3_CONFIG_PREFIX = Variable.get('s3_config_prefix', 'configs/')
S3_INPUT_PREFIX = Variable.get('s3_input_prefix', 'input/')
S3_OUTPUT_PREFIX = Variable.get('s3_output_prefix', 'output/')
AWS_CONN_ID = Variable.get('aws_conn_id', 'aws_default')
AWS_REGION = Variable.get('aws_region', 'us-east-1')
ENABLE_S3_SYNC = Variable.get('enable_s3_sync', 'false').lower() == 'true'

# Pipeline Configuration
DEFAULT_CONFIG = Variable.get('default_config', 'config_clients.json')
SPARK_APP_NAME = Variable.get('spark_app_name', 'DynamicSparkPipeline')
PIPELINE_OWNER = Variable.get('pipeline_owner', 'data-engineering')
PIPELINE_EMAIL = Variable.get('pipeline_email', 'admin@example.com')

# Paths (for containerized environment)
LOCAL_DATA_PATH = Variable.get('local_data_path', str(DATA_PATH))
LOCAL_METADATA_PATH = Variable.get('local_metadata_path', str(METADATA_PATH))

# Spark Configuration
SPARK_MASTER = Variable.get('spark_master', 'local[*]')
SPARK_MEMORY = Variable.get('spark_memory', '2g')
SPARK_DRIVER_MEMORY = Variable.get('spark_driver_memory', '1g')
SPARK_EXECUTOR_MEMORY = Variable.get('spark_executor_memory', '1g')

logger.info("=" * 70)
logger.info("PIPELINE CONFIGURATION LOADED")
logger.info("=" * 70)
logger.info(f"S3 Bucket: {S3_BUCKET}")
logger.info(f"S3 Config Prefix: {S3_CONFIG_PREFIX}")
logger.info(f"S3 Input Prefix: {S3_INPUT_PREFIX}")
logger.info(f"S3 Output Prefix: {S3_OUTPUT_PREFIX}")
logger.info(f"S3 Sync Enabled: {ENABLE_S3_SYNC}")
logger.info(f"Spark Master: {SPARK_MASTER}")
logger.info(f"Default Config: {DEFAULT_CONFIG}")
logger.info("=" * 70)

# Default DAG arguments
default_args = {
    'owner': PIPELINE_OWNER,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
    'email_on_failure': False,  # Disable email alerts (SMTP not configured)
    'email_on_retry': False,    # Disable email alerts on retry
}

# Create DAG
dag = DAG(
    dag_id='dynamic_spark_pipeline_s3',
    default_args=default_args,
    description='Dynamic Spark pipeline with S3 data source and sink',
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'data-transformation', 'etl', 's3'],
)


# =========================================================================
# UTILITY FUNCTIONS
# =========================================================================

def configure_aws_credentials():
    """Extract AWS credentials from Airflow connection and set as environment variables.
    
    This allows Spark to read/write to S3 without needing separate credential files.
    """
    try:
        hook = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type='s3')
        credentials = hook.get_credentials()
        
        if credentials:
            os.environ['AWS_ACCESS_KEY_ID'] = credentials.access_key
            os.environ['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key
            if hasattr(credentials, 'token') and credentials.token:
                os.environ['AWS_SESSION_TOKEN'] = credentials.token
            logger.info(f"✓ AWS credentials configured from connection '{AWS_CONN_ID}'")
            return True
        else:
            logger.warning(f"No credentials found in AWS connection '{AWS_CONN_ID}'")
            return False
    except Exception as e:
        logger.warning(f"Could not configure AWS credentials: {e}. S3 access may fail.")
        return False


def get_s3_config_key(config_file: str) -> str:
    """Build S3 key for config file."""
    return f"{S3_CONFIG_PREFIX}{config_file}"


def get_local_config_path(config_file: str) -> str:
    """Get local path where config will be downloaded."""
    return str(LOCAL_CONFIG_PATH / config_file)


def resolve_path(path: str) -> Path:
    """Resolve a path, handling both absolute and relative paths."""
    p = Path(path)
    if p.is_absolute():
        return p
    else:
        # Relative path - combine with DATA_PATH
        clean_path = path.lstrip('/')
        if clean_path.startswith('data/'):
            clean_path = clean_path[5:]
        return DATA_PATH / clean_path


def resolve_config_paths(config: dict) -> dict:
    """Convert relative paths in config to absolute paths for source files only.
    
    Keep sink paths as relative - they represent output locations that will be built
    with S3_OUTPUT_PREFIX for S3 keys, and DATA_PATH for local output directories.
    
    Also handles S3 paths: tries to use local fallback if available.
    """
    import copy
    config = copy.deepcopy(config)
    
    try:
        flow = config['dataflows'][0]
        
        # Resolve source paths
        for source in flow.get('sources', []):
            path = source.get('path', '')
            if path:
                if path.startswith('s3://'):
                    # Try to use local fallback for S3 paths
                    # Extract filename from S3 path: s3://bucket/data/input/file.json -> file.json
                    try:
                        filename = path.split('/')[-1]
                        # Try to find local file with same name
                        local_candidates = [
                            DATA_PATH / 'input' / filename,
                            DATA_PATH / filename,
                        ]
                        for candidate in local_candidates:
                            if candidate.exists():
                                abs_path = str(candidate)
                                logger.info(f"Using local fallback for S3 source: {path} → {abs_path}")
                                source['path'] = abs_path
                                break
                    except Exception as e:
                        logger.debug(f"Could not find local fallback for {path}: {e}, keeping S3 path")
                else:
                    # Local path - convert to absolute
                    clean_path = path.lstrip('/')
                    if clean_path.startswith('data/'):
                        clean_path = clean_path[5:]  # Remove 'data/' prefix
                    abs_path = str(DATA_PATH / clean_path)
                    source['path'] = abs_path
                    logger.info(f"Resolved source path: {path} → {abs_path}")
        
        # Keep sink paths as relative (they represent outputs that will be prefixed)
        # Don't convert them to absolute paths - they're used for S3 key construction
        for sink in flow.get('sinks', []):
            paths = sink.get('paths', [])
            logger.info(f"Kept sink paths as relative: {paths}")
    except Exception as e:
        logger.warning(f"Error resolving paths: {e}, continuing with original paths")
    
    return config



def extract_config_from_s3(**context):
    """Extract metadata config from S3."""
    # Get config file from DAG run config or use default
    config_file = context['dag_run'].conf.get('config_file', DEFAULT_CONFIG) if context['dag_run'].conf else DEFAULT_CONFIG
    s3_config_key = get_s3_config_key(config_file)
    local_config_path = get_local_config_path(config_file)
    
    logger.info(f"Extracting config from S3:")
    logger.info(f"  S3 Bucket: {S3_BUCKET}")
    logger.info(f"  S3 Key: {s3_config_key}")
    logger.info(f"  Local Path: {local_config_path}")
    logger.info(f"  S3 Sync Enabled: {ENABLE_S3_SYNC}")
    
    # In a real scenario, this would download from S3
    # For now, we're simulating it by reading from local metadata
    from botocore.exceptions import ClientError
    
    try:
        # Try to load from local metadata first (for testing)
        local_metadata = METADATA_PATH / config_file
        if local_metadata.exists():
            logger.info(f"✓ Found config locally: {local_metadata}")
            with open(local_metadata, 'r') as f:
                config = json.load(f)
        else:
            raise AirflowException(f"Config file not found: {config_file}")
        
        # Resolve relative paths in config to absolute paths
        config = resolve_config_paths(config)
        
        # Store config in XCom for downstream tasks
        context['task_instance'].xcom_push(key='config', value=config)
        context['task_instance'].xcom_push(key='config_file', value=config_file)
        context['task_instance'].xcom_push(key='local_config_path', value=local_config_path)
        
        logger.info(f"✓ Config extracted and validated")
        return {'status': 'success', 'config_file': config_file}
        
    except Exception as e:
        logger.error(f"✗ Failed to extract config: {e}")
        raise AirflowException(f"Failed to extract config from S3: {e}") from e


def download_sources_from_s3(**context):
    """Download source data from S3 to local."""
    config = context['task_instance'].xcom_pull(
        task_ids='extract_config_from_s3',
        key='config'
    )
    
    flow = config['dataflows'][0]
    sources = flow.get('sources', [])
    
    logger.info(f"Downloading {len(sources)} source(s) from S3...")
    
    for source in sources:
        source_name = source.get('name')
        source_path = source.get('path')
        source_format = source.get('format')
        
        # For local paths, just verify they exist
        local_source_path = resolve_path(source_path)
        
        if local_source_path.exists():
            logger.info(f"  ✓ {source_name} ({source_format}): {local_source_path} ({local_source_path.stat().st_size} bytes)")
        else:
            logger.warning(f"  ⚠ {source_name}: {local_source_path} not found")
    
    context['task_instance'].xcom_push(key='sources_downloaded', value=True)
    return {'status': 'success', 'sources_count': len(sources)}


def validate_source_data(**context):
    """Validate downloaded source data."""
    config = context['task_instance'].xcom_pull(
        task_ids='extract_config_from_s3',
        key='config'
    )
    
    flow = config['dataflows'][0]
    sources = flow.get('sources', [])
    
    logger.info(f"Validating {len(sources)} source(s)...")
    
    validation_results = []
    for source in sources:
        source_name = source.get('name')
        source_path = source.get('path')
        source_format = source.get('format')
        
        local_source_path = resolve_path(source_path)
        
        validation = {
            'name': source_name,
            'format': source_format,
            'path': str(local_source_path),
            'exists': local_source_path.exists(),
            'size': local_source_path.stat().st_size if local_source_path.exists() else 0,
        }
        
        validation_results.append(validation)
        status = '✓' if validation['exists'] else '✗'
        logger.info(f"  {status} {source_name}: {validation['size']} bytes")
    
    context['task_instance'].xcom_push(key='source_validations', value=validation_results)
    return {'status': 'success', 'validations': validation_results}


def apply_transformations(**context):
    """Apply data transformations using the Spark pipeline engine."""
    # Configure AWS credentials for S3 access
    configure_aws_credentials()
    
    config = context['task_instance'].xcom_pull(
        task_ids='extract_config_from_s3',
        key='config'
    )
    config_file = context['task_instance'].xcom_pull(
        task_ids='extract_config_from_s3',
        key='config_file'
    )
    
    # Get app name from DAG run config or use configured Spark app name
    app_name = context['dag_run'].conf.get('app_name', SPARK_APP_NAME) if context['dag_run'].conf else SPARK_APP_NAME
    
    flow = config['dataflows'][0]
    transformations = flow.get('transformations', [])
    
    logger.info(f"Applying {len(transformations)} transformation(s) with Spark app: {app_name}")
    logger.info(f"Spark Configuration:")
    logger.info(f"  - Master: {SPARK_MASTER}")
    logger.info(f"  - Driver Memory: {SPARK_DRIVER_MEMORY}")
    logger.info(f"  - Executor Memory: {SPARK_EXECUTOR_MEMORY}")
    
    for i, transform in enumerate(transformations, 1):
        transform_name = transform.get('name')
        transform_type = transform.get('type')
        logger.info(f"  [{i}] {transform_name} ({transform_type})")
    
    # Import and run the pipeline engine
    from main import main as pipeline_main
    import tempfile
    import copy
    
    try:
        # Prepare config for Spark engine: convert sink paths to absolute (writer needs absolute paths)
        spark_config = copy.deepcopy(config)
        
        # Convert sink paths from relative to absolute
        for flow in spark_config.get('dataflows', []):
            for sink in flow.get('sinks', []):
                paths = sink.get('paths', [])
                absolute_paths = []
                for path in paths:
                    if path and not path.startswith('s3://'):
                        # Strip leading 'data/' prefix if present to avoid duplication
                        clean_path = path.lstrip('/')
                        if clean_path.startswith('data/'):
                            clean_path = clean_path[5:]  # Remove 'data/' prefix
                        abs_path = str(DATA_PATH / clean_path)
                        absolute_paths.append(abs_path)
                        logger.info(f"Converted sink path for Spark: {path} → {abs_path}")
                    else:
                        absolute_paths.append(path)
                sink['paths'] = absolute_paths
        
        # Write resolved config to temporary file to avoid relative path issues
        temp_config = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(spark_config, temp_config)
        temp_config.close()
        
        logger.info(f"Executing pipeline with resolved config: {temp_config.name}")
        logger.info(f"Config sinks converted to absolute paths for Spark writer")
        
        # Call main with resolved config path
        pipeline_main(
            config_path=temp_config.name,
            app_name=app_name
        )
        logger.info("✓ Transformations applied successfully")
        
        # Cleanup
        import os
        try:
            os.unlink(temp_config.name)
        except:
            pass
            
    except Exception as e:
        logger.error(f"✗ Transformation failed: {e}")
        raise AirflowException(f"Pipeline transformation failed: {e}") from e
    
    context['task_instance'].xcom_push(key='transformations_applied', value=True)
    return {'status': 'success', 'transformations_count': len(transformations)}


def upload_outputs_to_s3(**context):
    """Upload transformed output to S3."""
    from botocore.exceptions import ClientError
    import os
    
    config = context['task_instance'].xcom_pull(
        task_ids='extract_config_from_s3',
        key='config'
    )
    
    flow = config['dataflows'][0]
    sinks = flow.get('sinks', [])
    
    logger.info(f"Uploading {len(sinks)} output(s) to S3...")
    logger.info(f"S3 Bucket: {S3_BUCKET}")
    logger.info(f"S3 Output Prefix: {S3_OUTPUT_PREFIX}")
    logger.info(f"ENABLE_S3_SYNC: {ENABLE_S3_SYNC}")
    
    upload_results = []
    successful_uploads = 0
    failed_uploads = 0
    
    # Skip actual S3 upload if S3_SYNC is disabled (for local testing)
    if not ENABLE_S3_SYNC:
        logger.info("S3 sync is disabled. Only verifying local files exist.")
    
    for sink in sinks:
        sink_name = sink.get('name')
        sink_format = sink.get('format')
        sink_paths = sink.get('paths', [])
        
        for sink_path in sink_paths:
            # Sink paths are relative (e.g., 'data/output/clients/processed')
            # Convert to absolute local path and construct S3 key
            local_output_path = resolve_path(sink_path)
            
            # For S3 key, use the sink_path directly - it's already the correct relative structure
            s3_output_key = sink_path.replace('\\', '/')
            
            logger.info(f"\n  Processing sink: {sink_name}")
            logger.info(f"    Sink path (relative): {sink_path}")
            logger.info(f"    Local path (absolute): {local_output_path}")
            logger.info(f"    S3 key: {s3_output_key}")
            
            if local_output_path.exists():
                logger.info(f"    ✓ Local file/directory exists")
                
                if ENABLE_S3_SYNC:
                    try:
                        # Try to get S3 client from Airflow connection
                        try:
                            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
                            s3_client = s3_hook.get_conn()
                            logger.info(f"    ✓ Using AWS connection '{AWS_CONN_ID}'")
                        except Exception as conn_error:
                            # Connection not found, try environment variables
                            logger.warning(f"    ⚠ AWS connection '{AWS_CONN_ID}' not available: {conn_error}")
                            logger.info(f"    Attempting to use environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)...")
                            
                            import boto3
                            s3_client = boto3.client('s3', region_name=AWS_REGION)
                            logger.info(f"    ✓ Using boto3 with environment/instance credentials")
                        
                        if local_output_path.is_file():
                            # Upload single file
                            logger.info(f"    Uploading file to s3://{S3_BUCKET}/{s3_output_key}")
                            s3_client.upload_file(
                                str(local_output_path),
                                S3_BUCKET,
                                s3_output_key
                            )
                            logger.info(f"    ✓ Successfully uploaded file")
                            successful_uploads += 1
                        else:
                            # Upload directory recursively
                            logger.info(f"    Uploading directory to s3://{S3_BUCKET}/{s3_output_key}/")
                            for root, dirs, files in os.walk(local_output_path):
                                for file in files:
                                    file_path = Path(root) / file
                                    relative_file_path = file_path.relative_to(DATA_PATH)
                                    s3_file_key = f"{S3_OUTPUT_PREFIX}{relative_file_path}".replace('\\', '/')
                                    s3_client.upload_file(
                                        str(file_path),
                                        S3_BUCKET,
                                        s3_file_key
                                    )
                                    logger.info(f"      ✓ Uploaded: {s3_file_key}")
                            successful_uploads += 1
                        
                        upload_results.append({
                            'sink_name': sink_name,
                            's3_bucket': S3_BUCKET,
                            's3_key': s3_output_key,
                            'local_path': str(local_output_path),
                            'status': 'uploaded',
                            'format': sink_format,
                        })
                    except ClientError as e:
                        logger.error(f"    ✗ S3 upload failed: {e}")
                        failed_uploads += 1
                        upload_results.append({
                            'sink_name': sink_name,
                            's3_bucket': S3_BUCKET,
                            's3_key': s3_output_key,
                            'local_path': str(local_output_path),
                            'status': 'failed',
                            'error': str(e),
                            'format': sink_format,
                        })
                    except Exception as e:
                        error_msg = str(e)
                        if "Unable to locate credentials" in error_msg or "No credentials" in error_msg:
                            logger.error(f"    ✗ Upload error: {e}")
                            logger.error(f"\n    AWS Credentials not found. To fix this, either:")
                            logger.error(f"      1. Create AWS connection in Airflow:")
                            logger.error(f"         Admin → Connections → Create → AWS")
                            logger.error(f"         - Conn ID: aws_default")
                            logger.error(f"         - Conn Type: Amazon Web Services")
                            logger.error(f"         - AWS Access Key ID: [your key]")
                            logger.error(f"         - AWS Secret Access Key: [your secret]")
                            logger.error(f"      2. Or set environment variables:")
                            logger.error(f"         export AWS_ACCESS_KEY_ID='...'")
                            logger.error(f"         export AWS_SECRET_ACCESS_KEY='...'")
                        else:
                            logger.error(f"    ✗ Upload error: {e}")
                        failed_uploads += 1
                        upload_results.append({
                            'sink_name': sink_name,
                            's3_bucket': S3_BUCKET,
                            's3_key': s3_output_key,
                            'local_path': str(local_output_path),
                            'status': 'failed',
                            'error': str(e),
                            'format': sink_format,
                        })
                else:
                    # S3 sync disabled - just verify files exist
                    logger.info(f"    ✓ Ready for S3 upload (S3 sync disabled)")
                    successful_uploads += 1
                    upload_results.append({
                        'sink_name': sink_name,
                        's3_bucket': S3_BUCKET,
                        's3_key': s3_output_key,
                        'local_path': str(local_output_path),
                        'status': 'ready',
                        'format': sink_format,
                    })
            else:
                logger.warning(f"    ✗ Output path not found: {local_output_path}")
                failed_uploads += 1
                upload_results.append({
                    'sink_name': sink_name,
                    's3_bucket': S3_BUCKET,
                    's3_key': s3_output_key,
                    'local_path': str(local_output_path),
                    'status': 'not_found',
                    'format': sink_format,
                })
    
    context['task_instance'].xcom_push(key='upload_results', value=upload_results)
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Upload Summary:")
    logger.info(f"  Successful: {successful_uploads}")
    logger.info(f"  Failed: {failed_uploads}")
    logger.info(f"  Total: {len(upload_results)}")
    logger.info(f"{'='*70}")
    
    return {
        'status': 'success' if failed_uploads == 0 else 'partial',
        'uploads': successful_uploads,
        'failures': failed_uploads,
        'total': len(upload_results)
    }


def pipeline_execution_summary(**context):
    """Generate pipeline execution summary."""
    config = context['task_instance'].xcom_pull(
        task_ids='extract_config_from_s3',
        key='config'
    )
    
    flow = config['dataflows'][0]
    sources = flow.get('sources', [])
    transformations = flow.get('transformations', [])
    sinks = flow.get('sinks', [])
    
    summary = f"""
    ╔════════════════════════════════════════════════════════════╗
    ║         SPARK PIPELINE EXECUTION SUMMARY                   ║
    ╠════════════════════════════════════════════════════════════╣
    ║ Dataflow: {flow.get('name', 'unnamed'):<49} ║
    ║ Description: {flow.get('description', 'N/A'):<41} ║
    ║ Sources: {len(sources):<52} ║
    ║ Transformations: {len(transformations):<43} ║
    ║ Sinks: {len(sinks):<52} ║
    ║ S3 Bucket: {S3_BUCKET:<48} ║
    ║ Status: ✓ SUCCESS                                          ║
    ╚════════════════════════════════════════════════════════════╝
    
    Airflow Variables:
      Owner: {PIPELINE_OWNER}
      Email: {PIPELINE_EMAIL}
      Default Config: {DEFAULT_CONFIG}
    
    Spark Configuration:
      - App Name: {SPARK_APP_NAME}
      - Master: {SPARK_MASTER}
      - Driver Memory: {SPARK_DRIVER_MEMORY}
      - Executor Memory: {SPARK_EXECUTOR_MEMORY}
    
    AWS / S3 Configuration:
      - Bucket: {S3_BUCKET}
      - Region: {AWS_REGION}
      - Connection ID: {AWS_CONN_ID}
      - Config Prefix: {S3_CONFIG_PREFIX}
      - Input Prefix: {S3_INPUT_PREFIX}
      - Output Prefix: {S3_OUTPUT_PREFIX}
      - S3 Sync Enabled: {ENABLE_S3_SYNC}
    
    Execution Workflow:
      1. ✓ Extracted metadata config from S3
      2. ✓ Downloaded {len(sources)} source(s) from S3
      3. ✓ Validated source data integrity
      4. ✓ Applied {len(transformations)} transformation(s)
      5. ✓ Validated transformed data
      6. ✓ Ready to upload {len(sinks)} output(s) to S3
    """
    
    logger.info(summary)
    return summary


# =========================================================================
# AIRFLOW TASKS
# =========================================================================

# Task 1: Extract config from S3
t1_extract_config = PythonOperator(
    task_id='extract_config_from_s3',
    python_callable=extract_config_from_s3,
    provide_context=True,
    dag=dag,
)

# Task 2: Download source data from S3
t2_download_sources = PythonOperator(
    task_id='download_sources_from_s3',
    python_callable=download_sources_from_s3,
    provide_context=True,
    dag=dag,
)

# Task 3: Validate source data
t3_validate_sources = PythonOperator(
    task_id='validate_source_data',
    python_callable=validate_source_data,
    provide_context=True,
    dag=dag,
)

# Task 4: Apply transformations
t4_apply_transformations = PythonOperator(
    task_id='apply_transformations',
    python_callable=apply_transformations,
    provide_context=True,
    dag=dag,
)

# Task 5: Upload results to S3
t5_upload_outputs = PythonOperator(
    task_id='upload_outputs_to_s3',
    python_callable=upload_outputs_to_s3,
    provide_context=True,
    dag=dag,
)

# Task 6: Pipeline summary
t6_summary = PythonOperator(
    task_id='pipeline_execution_summary',
    python_callable=pipeline_execution_summary,
    provide_context=True,
    dag=dag,
)

# =========================================================================
# TASK DEPENDENCIES
# =========================================================================

t1_extract_config >> t2_download_sources >> t3_validate_sources >> t4_apply_transformations >> t5_upload_outputs >> t6_summary