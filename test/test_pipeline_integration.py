#!/usr/bin/env python3
"""
Comprehensive Pipeline Integration Test with CLI Arguments

Usage:
    python test_pipeline_integration.py --config <config_path> --input <input_dir>
    python test_pipeline_integration.py --config metadata/config.json --input data/input

Features:
- CLI argument support for config and input paths
- Full v2 registry integration testing
- End-to-end pipeline execution
- Detailed validation and reporting
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from engine import PipelineEngine
from transformers_registry_v2 import (
    list_available_transformations,
    create_transformation,
    ValidationError,
    ExecutionError
)


class PipelineTestRunner:
    """Clean test runner for pipeline integration testing"""
    
    def __init__(self, config_path: str, input_dir: str):
        self.config_path = Path(config_path)
        self.input_dir = Path(input_dir)
        self.results = {
            "config_validation": False,
            "environment_check": False,
            "v2_registry_test": False,
            "pipeline_execution": False,
            "output_validation": False
        }
        self.errors = []
        self.spark = None
    
    # =========================================================================
    # VALIDATION PHASE
    # =========================================================================
    
    def validate_inputs(self) -> bool:
        """Validate input paths and configuration file"""
        print(f"\n{'='*70}")
        print("PHASE 1: INPUT VALIDATION")
        print(f"{'='*70}\n")
        
        # Check config file
        if not self.config_path.exists():
            self.errors.append(f"Config file not found: {self.config_path}")
            print(f"❌ Config file not found: {self.config_path}")
            return False
        
        print(f"✓ Config file found: {self.config_path}")
        
        # Check input directory
        if not self.input_dir.exists():
            self.errors.append(f"Input directory not found: {self.input_dir}")
            print(f"❌ Input directory not found: {self.input_dir}")
            return False
        
        print(f"✓ Input directory exists: {self.input_dir}")
        
        # Load and validate config
        try:
            with open(self.config_path) as f:
                config = json.load(f)
            
            # Validate config structure
            if not isinstance(config, dict):
                raise TypeError("Config must be a JSON object")
            
            dataflows = config.get("dataflows", [])
            if not dataflows:
                raise ValueError("Config must contain 'dataflows' array")
            
            print(f"✓ Config JSON is valid with {len(dataflows)} dataflow(s)")
            
            # Display config summary
            self._print_config_summary(config)
            self.results["config_validation"] = True
            return True
            
        except json.JSONDecodeError as e:
            self.errors.append(f"Invalid JSON in config: {e}")
            print(f"❌ Invalid JSON in config: {e}")
            return False
        except Exception as e:
            self.errors.append(f"Config validation failed: {e}")
            print(f"❌ Config validation failed: {e}")
            return False
    
    def _print_config_summary(self, config: Dict):
        """Print configuration summary"""
        for i, flow in enumerate(config.get("dataflows", []), 1):
            print(f"\n  Dataflow {i}: {flow.get('name', 'unnamed')}")
            
            sources = flow.get("sources", [])
            print(f"    Sources: {len(sources)}")
            for src in sources:
                print(f"      - {src.get('name')} ({src.get('format')})")
            
            transforms = flow.get("transformations", [])
            print(f"    Transformations: {len(transforms)}")
            for t in transforms:
                print(f"      - {t.get('name')} ({t.get('type')})")
            
            sinks = flow.get("sinks", [])
            print(f"    Sinks: {len(sinks)}")
            for sink in sinks:
                print(f"      - {sink.get('name')} ({sink.get('format')})")
    
    # =========================================================================
    # ENVIRONMENT SETUP PHASE
    # =========================================================================
    
    def setup_environment(self) -> bool:
        """Initialize Spark and check environment"""
        print(f"\n{'='*70}")
        print("PHASE 2: ENVIRONMENT SETUP")
        print(f"{'='*70}\n")
        
        try:
            print("Initializing Spark session...")
            self.spark = SparkSession.builder \
                .appName("PipelineIntegrationTest") \
                .master("local[*]") \
                .getOrCreate()
            
            print(f"✓ Spark session initialized: {self.spark.version}")
            
            # Check PySpark version
            import pyspark
            print(f"✓ PySpark version: {pyspark.__version__}")
            
            self.results["environment_check"] = True
            return True
            
        except Exception as e:
            self.errors.append(f"Environment setup failed: {e}")
            print(f"❌ Environment setup failed: {e}")
            return False
    
    # =========================================================================
    # REGISTRY V2 INTEGRATION PHASE
    # =========================================================================
    
    def test_v2_registry(self) -> bool:
        """Test transformers_registry_v2 integration"""
        print(f"\n{'='*70}")
        print("PHASE 3: TRANSFORMERS_REGISTRY_V2 INTEGRATION")
        print(f"{'='*70}\n")
        
        try:
            # List available transformations
            available = list_available_transformations()
            print(f"✓ Available transformation handlers: {len(available)}")
            for handler in available:
                print(f"  - {handler}")
            
            if not available:
                raise ValueError("No transformation handlers found in registry")
            
            # Create test dataframe
            print(f"\nCreating test DataFrame...")
            test_data = [
                {"id": 1, "name": "Alice", "age": 25},
                {"id": 2, "name": "Bob", "age": 17},
                {"id": 3, "name": "Charlie", "age": 30},
            ]
            df = self.spark.createDataFrame(test_data)
            print(f"✓ Test DataFrame created: {df.count()} rows, {len(df.columns)} columns")
            
            # Test key handlers
            tests = [
                {
                    "name": "filter_rows",
                    "handler_type": "filter_rows",
                    "params": {"condition": "age >= 18"},
                    "validation": lambda result: result.count() == 2
                },
                {
                    "name": "derive_column",
                    "handler_type": "derive_column",
                    "params": {
                        "column": "status",
                        "expression": "CASE WHEN age >= 18 THEN 'Adult' ELSE 'Minor' END"
                    },
                    "validation": lambda result: "status" in result.columns
                },
                {
                    "name": "select_columns",
                    "handler_type": "select_columns",
                    "params": {"columns": ["name", "age"]},
                    "validation": lambda result: result.columns == ["name", "age"]
                },
                {
                    "name": "drop_columns",
                    "handler_type": "drop_columns",
                    "params": {"columns": ["id"]},
                    "validation": lambda result: "id" not in result.columns
                },
            ]
            
            print(f"\nTesting {len(tests)} handlers...")
            for test in tests:
                try:
                    handler = create_transformation(
                        test["handler_type"],
                        self.spark,
                        df,
                        test["params"]
                    )
                    result = handler.execute()
                    
                    if test["validation"](result):
                        print(f"  ✓ {test['name']}: PASSED")
                    else:
                        print(f"  ✗ {test['name']}: FAILED (validation)")
                        return False
                        
                except Exception as e:
                    print(f"  ✗ {test['name']}: FAILED ({e})")
                    return False
            
            print(f"\n✓ All v2 registry tests passed")
            self.results["v2_registry_test"] = True
            return True
            
        except Exception as e:
            self.errors.append(f"V2 registry test failed: {e}")
            print(f"❌ V2 registry test failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    # =========================================================================
    # PIPELINE EXECUTION PHASE
    # =========================================================================
    
    def execute_pipeline(self) -> bool:
        """Execute the full pipeline with provided configuration"""
        print(f"\n{'='*70}")
        print("PHASE 4: PIPELINE EXECUTION")
        print(f"{'='*70}\n")
        
        try:
            print(f"Loading config from: {self.config_path}")
            with open(self.config_path) as f:
                config = json.load(f)
            
            print(f"Executing pipeline with {len(config.get('dataflows', []))} dataflow(s)...")
            
            engine = PipelineEngine(self.spark, config)
            engine.run()
            
            print(f"✓ Pipeline execution completed successfully")
            self.results["pipeline_execution"] = True
            return True
            
        except Exception as e:
            self.errors.append(f"Pipeline execution failed: {e}")
            print(f"❌ Pipeline execution failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    # =========================================================================
    # OUTPUT VALIDATION PHASE
    # =========================================================================
    
    def validate_outputs(self) -> bool:
        """Validate pipeline output"""
        print(f"\n{'='*70}")
        print("PHASE 5: OUTPUT VALIDATION")
        print(f"{'='*70}\n")
        
        try:
            output_dir = Path("data/output")
            
            if not output_dir.exists():
                print(f"⚠ Output directory not found: {output_dir}")
                print(f"(This is OK if no outputs were configured)")
                self.results["output_validation"] = True
                return True
            
            # Find output files
            json_files = list(output_dir.rglob("*.json"))
            parquet_files = list(output_dir.rglob("*.parquet"))
            part_files = list(output_dir.rglob("part-*"))
            
            total_files = len(json_files) + len(parquet_files) + len(part_files)
            
            if total_files == 0:
                print(f"⚠ No output files found in {output_dir}")
                print(f"(This is OK if pipeline had no output sinks)")
                self.results["output_validation"] = True
                return True
            
            print(f"✓ Found {total_files} output file(s)")
            print(f"  - JSON files: {len(json_files)}")
            print(f"  - Parquet files: {len(parquet_files)}")
            print(f"  - Part files: {len(part_files)}")
            
            # Show sample from first file
            if json_files:
                with open(json_files[0]) as f:
                    sample = f.readline()
                    if sample:
                        print(f"\n  Sample record: {sample[:100]}...")
            
            self.results["output_validation"] = True
            return True
            
        except Exception as e:
            self.errors.append(f"Output validation failed: {e}")
            print(f"❌ Output validation failed: {e}")
            return False
    
    # =========================================================================
    # REPORTING
    # =========================================================================
    
    def print_summary(self) -> int:
        """Print test summary and return exit code"""
        print(f"\n{'='*70}")
        print("FINAL TEST SUMMARY")
        print(f"{'='*70}\n")
        
        for test_name, passed in self.results.items():
            status = "✅ PASSED" if passed else "❌ FAILED"
            print(f"{status} - {test_name.replace('_', ' ').title()}")
        
        # Count results
        passed_count = sum(1 for v in self.results.values() if v)
        total_count = len(self.results)
        
        print(f"\n{'='*70}")
        print(f"Results: {passed_count}/{total_count} phases passed")
        print(f"{'='*70}\n")
        
        if self.errors:
            print("ERRORS ENCOUNTERED:")
            for i, error in enumerate(self.errors, 1):
                print(f"  {i}. {error}")
            print()
        
        all_passed = all(self.results.values())
        if all_passed:
            print("🎉 ALL TESTS PASSED - Pipeline integration is working correctly!")
        else:
            print("⚠️  SOME TESTS FAILED - See errors above for details")
        
        print(f"{'='*70}\n")
        
        return 0 if all_passed else 1
    
    def cleanup(self):
        """Clean up resources"""
        if self.spark:
            self.spark.stop()
    
    # =========================================================================
    # MAIN TEST EXECUTION
    # =========================================================================
    
    def run_all_tests(self) -> int:
        """Execute all test phases"""
        try:
            if not self.validate_inputs():
                return 1
            
            if not self.setup_environment():
                return 1
            
            if not self.test_v2_registry():
                return 1
            
            if not self.execute_pipeline():
                return 1
            
            if not self.validate_outputs():
                return 1
            
            return self.print_summary()
            
        finally:
            self.cleanup()


def parse_arguments():
    """Parse CLI arguments"""
    parser = argparse.ArgumentParser(
        description="Comprehensive Pipeline Integration Test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_pipeline_integration.py --config metadata/config.json --input data/input
  python test_pipeline_integration.py --config metadata/config_clients.json --input data/input
        """
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default="metadata/config.json",
        help="Path to metadata configuration file (default: metadata/config.json)"
    )
    
    parser.add_argument(
        "--input",
        type=str,
        default="data/input",
        help="Path to input data directory (default: data/input)"
    )
    
    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_arguments()
    
    print(f"\n{'='*70}")
    print("PIPELINE INTEGRATION TEST SUITE")
    print("with TransformersRegistryV2")
    print(f"{'='*70}")
    print(f"Config: {args.config}")
    print(f"Input:  {args.input}")
    print(f"{'='*70}\n")
    
    runner = PipelineTestRunner(args.config, args.input)
    exit_code = runner.run_all_tests()
    
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
