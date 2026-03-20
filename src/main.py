import json
from pathlib import Path

from pyspark.sql import SparkSession

from engine import PipelineEngine


def main():
    config_path = Path("metadata/config.json")
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open() as file:
        config = json.load(file)

    spark = SparkSession.builder.appName("DynamicPipeline").getOrCreate()

    try:
        engine = PipelineEngine(spark, config)
        engine.run()
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        raise SystemExit(f"Pipeline execution failed: {exc}") from exc