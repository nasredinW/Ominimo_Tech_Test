#!/usr/bin/env python
"""
Run the clients processing pipeline with advanced transformations.
Usage: python run_clients_pipeline.py
"""

import json
from pathlib import Path
from pyspark.sql import SparkSession
from src.engine import PipelineEngine
from src.logger import setup_logger

logger = setup_logger(__name__)


def main():
    logger.info("Starting Clients Processing Pipeline...")
    
    config_path = Path("metadata/config_clients.json")
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    logger.info(f"Loading config from {config_path}")
    with config_path.open() as file:
        config = json.load(file)

    logger.info(f"Creating Spark session...")
    spark = SparkSession.builder.appName("ClientsProcessingPipeline").getOrCreate()

    try:
        logger.info(f"Executing {len(config.get('dataflows', []))} dataflow(s)...")
        engine = PipelineEngine(spark, config)
        engine.run()
        logger.info("Pipeline completed successfully")
        logger.info("✓ Output files created:")
        logger.info("  - JSON: data/output/clients/processed/")
        logger.info("  - Parquet: data/output/clients/parquet/")
        logger.info("  - Rejected: data/output/clients/rejected/")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        raise
    finally:
        logger.info("Stopping Spark session...")
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.critical(f"Fatal error: {exc}")
        raise SystemExit(f"Pipeline execution failed: {exc}") from exc
