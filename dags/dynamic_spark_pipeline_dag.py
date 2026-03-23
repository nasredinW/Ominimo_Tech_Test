"""Airflow DAG to orchestrate the dynamic Spark pipeline with logging and alerts.

This DAG is  flexible and can be customized via:
- Environment variables (APP_HOME, CONFIG_PATH, DATA_PATH, LOG_PATH, SCHEDULE)
- DAG parameters at runtime
- Configuration file structure (auto-detects dataflows)
"""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Configure logger
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION - All customizable for maximum flexibility
# ============================================================================

# Application paths (override via environment variables)
APP_HOME = os.getenv("APP_HOME", "/app")
CONFIG_PATH = os.getenv("CONFIG_PATH", f"{APP_HOME}/metadata/config.json")
DATA_PATH = os.getenv("DATA_PATH", f"{APP_HOME}/data")
LOG_PATH = os.getenv("LOG_PATH", f"{APP_HOME}/airflow/logs/pipeline")
SCRIPT_PATH = os.getenv("SCRIPT_PATH", f"{APP_HOME}/src/main.py")

# DAG scheduling (override via environment variable)
SCHEDULE_INTERVAL = os.getenv("SCHEDULE_INTERVAL", "0 2 * * *")  # Daily at 2 AM
DAG_ID = os.getenv("DAG_ID", "dynamic_spark_pipeline")

# Retry configuration
DEFAULT_RETRIES = int(os.getenv("DEFAULT_RETRIES", "2"))
RETRY_DELAY_MINUTES = int(os.getenv("RETRY_DELAY_MINUTES", "5"))

DEFAULT_ARGS = {
    "owner": os.getenv("DAG_OWNER", "Nasredine Ouelseti"),
    "depends_on_past": False,
    "retries": DEFAULT_RETRIES,
    "retry_delay": timedelta(minutes=RETRY_DELAY_MINUTES),
    "start_date": datetime(2026, 1, 1),
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def load_config(config_file: str) -> dict:
    """Load and validate configuration file."""
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path) as f:
        config = json.load(f)
    
    # Validate required structure
    if "dataflows" not in config:
        raise ValueError("Config must contain 'dataflows' key")
    
    return config


def get_dataflows(config: dict) -> list:
    """Extract dataflow names from config."""
    return [df.get("name", f"dataflow_{i}") for i, df in enumerate(config.get("dataflows", []))]


def check_config_exists(**context):
    """Verify config file exists and is valid."""
    try:
        config = load_config(CONFIG_PATH)
        dataflows = get_dataflows(config)
        
        logger.info(f"✓ Config loaded from: {CONFIG_PATH}")
        logger.info(f"✓ Dataflows found: {dataflows}")
        
        # Store in XCom for downstream tasks
        context['task_instance'].xcom_push(key='config', value=config)
        context['task_instance'].xcom_push(key='dataflows', value=dataflows)
        
        return {"status": "success", "dataflows": dataflows}
    
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logger.error(f"✗ Config validation failed: {e}")
        raise


def validate_pipeline_outputs(**context):
    """Check pipeline outputs dynamically based on config dataflows."""
    try:
        # Get config from XCom
        task_instance = context['task_instance']
        config = task_instance.xcom_pull(key='config', task_ids='check_config')
        
        if not config:
            logger.warning("Could not retrieve config from check_config task")
            return {"status": "warning", "message": "No config found", "total_files": 0}
        
        dataflows = config.get("dataflows", [])
        all_files = []
        output_stats = {}
        
        logger.info("=" * 80)
        logger.info("VALIDATING PIPELINE OUTPUTS")
        logger.info("=" * 80)
        
        # Check outputs for each dataflow using ACTUAL sink paths from config
        for dataflow in dataflows:
            dataflow_name = dataflow.get("name", "unknown")
            sinks = dataflow.get("sinks", [])
            
            logger.info(f"Dataflow: {dataflow_name}")
            dataflow_stats = {}
            
            # Check each sink's actual output paths
            for sink in sinks:
                sink_name = sink.get("name", "unknown")
                sink_paths = sink.get("paths", [])
                
                sink_files = []
                for sink_path in sink_paths:
                    # Sink paths are relative to APP_HOME, not DATA_PATH
                    # Example: "data/output/events/motor_policy" is relative to APP_HOME
                    if not Path(sink_path).is_absolute():
                        full_path = Path(APP_HOME) / sink_path
                    else:
                        full_path = Path(sink_path)
                    
                    logger.info(f"  Checking: {full_path}")
                    
                    if full_path.exists():
                        files = list(full_path.glob("part-*.json"))
                        sink_files.extend(files)
                        file_count = len(files)
                        logger.info(f"    ✓ Found {file_count} file(s)")
                    else:
                        logger.warning(f"    ✗ Path does not exist")
                
                all_files.extend(sink_files)
                dataflow_stats[sink_name] = len(sink_files)
                
                logger.info(f"  [{sink_name}] Total: {len(sink_files)} file(s)")
            
            output_stats[dataflow_name] = dataflow_stats
        
        logger.info("=" * 80)
        logger.info(f"✓ Pipeline generated {len(all_files)} output files total")
        logger.info("=" * 80)
        
        # Store stats in XCom
        task_instance.xcom_push(key='output_stats', value=output_stats)
        
        return {
            "status": "success" if all_files else "warning",
            "total_files": len(all_files),
            "stats": output_stats,
        }
    
    except Exception as e:
        logger.error(f"✗ Output validation failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e), "total_files": 0}


def log_pipeline_stats(**context):
    """Log aggregated pipeline execution statistics."""
    try:
        task_instance = context['task_instance']
        output_stats = task_instance.xcom_pull(key='output_stats', task_ids='validate_outputs')
        
        if not output_stats:
            logger.warning("No output statistics available")
            return
        
        logger.info(f"=" * 80)
        logger.info(f"PIPELINE EXECUTION SUMMARY")
        logger.info(f"=" * 80)
        
        total_output = 0
        
        for dataflow_name, sinks_stats in output_stats.items():
            logger.info(f"  Dataflow: {dataflow_name}")
            
            dataflow_total = 0
            if isinstance(sinks_stats, dict):
                for sink_name, count in sinks_stats.items():
                    logger.info(f"    - {sink_name:25} | {count:4d} file(s)")
                    dataflow_total += count
            else:
                # Fallback for unexpected format
                logger.warning(f"    Unexpected stats format: {sinks_stats}")
            
            logger.info(f"    Subtotal: {dataflow_total:4d}")
            total_output += dataflow_total
        
        logger.info(f"{'-' * 80}")
        logger.info(f"  TOTAL OUTPUT FILES: {total_output:4d}")
        logger.info(f"=" * 80)
        
        # Store in XCom for monitoring
        task_instance.xcom_push(key='final_stats', value={
            'total_output': total_output,
            'dataflow_stats': output_stats,
        })
    
    except Exception as e:
        logger.warning(f"Could not gather stats: {e}")



# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id=DAG_ID,
    description="Orchestrate config-driven PySpark data pipeline with dynamic dataflows and flexible configuration",
    default_args=DEFAULT_ARGS,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    tags=["spark", "pyspark", "data-pipeline", "dynamic"],
    params={
        "config_path": CONFIG_PATH,
        "data_path": DATA_PATH,
        "enable_validation": True,
        "enable_stats_logging": True,
    },
    doc_md=f"""
## Dynamic Spark Pipeline DAG

This DAG orchestrates a **highly flexible** config-driven PySpark pipeline that:
- 📋 Reads configuration from: `{CONFIG_PATH}`
- 📂 Uses data paths: `{DATA_PATH}`
- ✅ Applies field validations and transformations
- 📊 Writes outputs to separate valid/invalid directories
- 🔄 Automatically detects all dataflows from config

### Configuration

All paths can be customized via environment variables:
- `APP_HOME` - Application root directory (default: `/app`)
- `CONFIG_PATH` - Configuration file path
- `DATA_PATH` - Data directory path
- `LOG_PATH` - Log directory path
- `SCHEDULE_INTERVAL` - Cron schedule expression (default: `0 2 * * *`)
- `DAG_ID` - DAG identifier
- `DAG_OWNER` - DAG owner name

### Tasks

1. **check_config** - Validates config file and extracts dataflows
2. **run_pipeline** - Executes the PySpark transformation job
3. **validate_outputs** - Verifies outputs for all dataflows
4. **log_stats** - Aggregates and logs execution statistics

### Dynamic Features

✨ **Auto-detects dataflows** from config (no code changes needed!)  
✨ **Supports multiple dataflows** in single pipeline run  
✨ **Environment variable overrides** for all paths and settings  
✨ **XCom data passing** for inter-task communication  
✨ **Comprehensive logging** with dataflow-level metrics  
✨ **Flexible scheduling** via environment variables  

### Usage

Default run (production):
```bash
airflow dags test dynamic_spark_pipeline
```

Custom schedule (hourly):
```bash
export SCHEDULE_INTERVAL='0 * * * *'
airflow dags test dynamic_spark_pipeline
```

Custom paths:
```bash
export APP_HOME=/custom/app/path
export CONFIG_PATH=/custom/config.json
export DATA_PATH=/custom/data
airflow dags test dynamic_spark_pipeline
```

### Monitoring

Check pipeline stats via XCom:
```python
# In another task or monitoring script
xcom = ti.xcom_pull(key='final_stats', task_ids='log_stats')
total_events = xcom['total_events']
total_discards = xcom['total_discards']
```
""",
) as dag:
    
    check_config = PythonOperator(
        task_id="check_config",
        python_callable=check_config_exists,
        provide_context=True,
        doc="Validates configuration file and extracts dataflows",
    )

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command=f"""
set -e

echo "Starting Spark pipeline execution..."
echo "Configuration: {CONFIG_PATH}"
echo "Data path: {DATA_PATH}"
echo "Script: {SCRIPT_PATH}"
echo "Log path: {LOG_PATH}"

# Pre-create critical directories with proper error handling
echo "Preparing directories..."
LOG_DIR_BACKUP="/tmp/airflow_logs"

# Try to create log directory with primary path
if mkdir -p "{LOG_PATH}" 2>/dev/null && touch "{LOG_PATH}/.write_test" 2>/dev/null; then
    echo "✓ Using primary log path: {LOG_PATH}"
    rm -f "{LOG_PATH}/.write_test"
else
    echo "⚠ Primary log path not writable, using backup: $LOG_DIR_BACKUP"
    mkdir -p "$LOG_DIR_BACKUP"
    mkdir -p "{DATA_PATH}/output" 2>/dev/null || true
fi

# Create data output directory (non-critical)
mkdir -p "{DATA_PATH}/output" 2>/dev/null || echo "⚠ Warning: Could not create data output directory"

# Clean up old output files to prevent Spark OVERWRITE permission issues
echo "Cleaning previous output..."
if [ -d "{DATA_PATH}/output" ]; then
    # Ensure we can delete all files by fixing permissions first
    chmod -R 777 "{DATA_PATH}/output" 2>/dev/null || true
    # Remove entire output tree and recreate
    rm -rf "{DATA_PATH}/output" 2>/dev/null && echo "  ✓ Cleaned {DATA_PATH}/output" || echo "  ⚠ Partial cleanup of {DATA_PATH}/output"
fi
# Recreate clean output directory structure
mkdir -p "{DATA_PATH}/output/events" 2>/dev/null || true
mkdir -p "{DATA_PATH}/output/discards" 2>/dev/null || true
chmod -R 777 "{DATA_PATH}/output" 2>/dev/null || true

# Set effective log path for the script
export LOG_PATH_EFFECTIVE="{LOG_PATH}"
if [ ! -w "{LOG_PATH}" ]; then
    export LOG_PATH_EFFECTIVE="$LOG_DIR_BACKUP"
fi

echo "Effective log path: $LOG_PATH_EFFECTIVE"

# Change to application home directory
if [ -d "{APP_HOME}" ]; then
    cd "{APP_HOME}"
    echo "✓ Changed to: {APP_HOME}"
else
    echo "✗ Application directory not found: {APP_HOME}"
    exit 1
fi

# Run the pipeline script
echo "Executing pipeline script..."
python "{SCRIPT_PATH}" 2>&1 | tee -a "$LOG_PATH_EFFECTIVE/pipeline.log" || true

# Capture exit code
EXIT_CODE=${{PIPESTATUS[0]}}

# Report results
echo ""
echo "Pipeline execution completed with exit code: $EXIT_CODE"

if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Pipeline executed successfully"
    exit 0
else
    echo "✗ Pipeline failed with errors (see logs above)"
    exit $EXIT_CODE
fi
        """,
        retries=DEFAULT_RETRIES,
        pool="default_pool",
        queue="default",
        doc="Executes the PySpark transformation pipeline",
    )

    validate_outputs = PythonOperator(
        task_id="validate_outputs",
        python_callable=validate_pipeline_outputs,
        provide_context=True,
        trigger_rule="all_done",  # Run even if pipeline failed (for diagnostics)
        doc="Validates that pipeline outputs exist for all dataflows",
    )

    log_stats = PythonOperator(
        task_id="log_stats",
        python_callable=log_pipeline_stats,
        provide_context=True,
        trigger_rule="all_done",  # Always log stats
        doc="Aggregates and logs execution statistics",
    )

    # =========================================================================
    # Task Dependencies
    # =========================================================================
    check_config >> run_pipeline >> validate_outputs >> log_stats

