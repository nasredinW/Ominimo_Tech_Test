#!/usr/bin/env python3

import argparse
import json
import os
import re
import sys
import boto3
from pathlib import Path
from typing import Optional, Union

from pyspark.sql import SparkSession

from engine import PipelineEngine
from logger import setup_logger

logger = setup_logger(__name__)


class ConfigResolver:
    """Handle config loading and environment variable resolution."""
    
    @staticmethod
    def resolve_placeholders(config: dict) -> dict:
        """Recursively resolve ${VAR_NAME} placeholders using environment variables."""
        def resolve_string(s: str) -> str:
            def replacer(match):
                var_name = match.group(1)
                value = os.getenv(var_name)
                if value is None:
                    logger.warning(f"Environment variable '{var_name}' not set, keeping placeholder")
                    return match.group(0)
                return value
            return re.sub(r'\$\{([^}]+)\}', replacer, s)
        
        def resolve_value(value):
            if isinstance(value, str):
                return resolve_string(value)
            elif isinstance(value, dict):
                return {k: resolve_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [resolve_value(item) for item in value]
            return value
        
        logger.info("Resolving environment variable placeholders in config...")
        resolved = resolve_value(config)
        
        return resolved
    
    @staticmethod
    def load_config(config_path: str) -> dict:
        """Load and resolve config from file (local or S3)."""
        logger.info(f"Loading config from {config_path}")
        
        # Handle S3 paths
        if config_path.startswith("s3://"):
            try:
                
                s3_client = boto3.client("s3")
                
                # Parse S3 path: s3://bucket/key
                parts = config_path.replace("s3://", "").split("/", 1)
                bucket = parts[0]
                key = parts[1] if len(parts) > 1 else ""
                
                if not key:
                    raise ValueError(f"Invalid S3 path: {config_path}")
                
                logger.info(f"Reading config from S3: bucket={bucket}, key={key}")
                response = s3_client.get_object(Bucket=bucket, Key=key)
                config = json.load(response["Body"])
            except ImportError:
                raise ImportError("boto3 is required for S3 config paths. Install it with: pip install boto3")
            except Exception as e:
                raise FileNotFoundError(f"Failed to load config from S3 ({config_path}): {e}")
        else:
            # Handle local file paths
            path = Path(config_path)
            if not path.exists():
                raise FileNotFoundError(f"Config file not found: {path}")
            
            with path.open() as f:
                config = json.load(f)
        
        return ConfigResolver.resolve_placeholders(config)


class SparkSessionBuilder:
    """Build and configure Spark session."""
    
    @staticmethod
    def build(app_name: str) -> SparkSession:
        """Create Spark session."""
        logger.info(f"Creating Spark session: {app_name}")
        return SparkSession.builder.appName(app_name).getOrCreate()


class PipelineRunner:
    """Orchestrate pipeline execution."""
    
    def __init__(self, config_path: str, app_name: str):
        self.config_path = config_path
        self.app_name = app_name
        self.spark = None
    
    def run(self):
        """Execute the pipeline."""
        try:
            logger.info("=" * 80)
            logger.info("STARTING DYNAMIC SPARK PIPELINE")
            logger.info("=" * 80)
            
            config = ConfigResolver.load_config(self.config_path)
            self.spark = SparkSessionBuilder.build(self.app_name)
            
            dataflows_count = len(config.get("dataflows", []))
            logger.info(f"Executing {dataflows_count} dataflow(s)...")
            
            engine = PipelineEngine(self.spark, config)
            engine.run()
            
            logger.info("=" * 80)
            logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
        
        except Exception as e:
            logger.error(f"✗ Pipeline execution failed: {e}", exc_info=True)
            raise
        
        finally:
            if self.spark:
                logger.info("Stopping Spark session...")
                self.spark.stop()


def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    """Parse command line arguments.
    
    Supports both:
    - Direct usage: python main.py --config config.json
    - S3FileTransformOperator: python main.py /tmp/input /tmp/output --config config.json
    """
    parser = argparse.ArgumentParser(
        description="Run the dynamic Spark pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    # Positional arguments (optional - passed by S3FileTransformOperator but not used)
    parser.add_argument(
        "input_file",
        nargs="?",
        default=None,
        help="Input file (optional, passed by S3FileTransformOperator but config is source of truth)",
    )
    
    parser.add_argument(
        "output_file",
        nargs="?",
        default=None,
        help="Output file (optional, passed by S3FileTransformOperator but config is source of truth)",
    )
    
    # Named arguments
    parser.add_argument(
        "--config",
        required=True,
        help="Path to pipeline config JSON (local or s3://)",
    )
    
    parser.add_argument(
        "--app-name",
        default="DynamicPipeline",
        help="Spark application name",
    )
    
    return parser.parse_args(argv)


def main(config_path: str, app_name: str = "DynamicPipeline") -> None:
    """Main entry point for the pipeline."""
    runner = PipelineRunner(config_path, app_name)
    runner.run()


if __name__ == "__main__":
    try:
        args = _parse_args()
        main(config_path=args.config, app_name=args.app_name)
    except Exception as exc:
        logger.critical(f"Fatal error: {exc}")
        sys.exit(1)
