import json
from pathlib import Path

from pyspark.sql import SparkSession

from engine import PipelineEngine
from logger import setup_logger

logger = setup_logger(__name__)


def main():
    logger.info("Starting Dynamic Spark Pipeline...")
    
    config_path = Path("metadata/config.json")
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    logger.info(f"Loading config from {config_path}")
    with config_path.open() as file:
        config = json.load(file)

    logger.info(f"Creating Spark session...")
    spark = SparkSession.builder.appName("DynamicPipeline").getOrCreate()

    try:
        logger.info(f"Executing {len(config.get('dataflows', []))} dataflow(s)...")
        engine = PipelineEngine(spark, config)
        engine.run()
        logger.info("Pipeline completed successfully")
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