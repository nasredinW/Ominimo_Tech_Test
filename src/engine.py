"""
PySpark Pipeline Engine - Orchestrates data transformation workflows.

Responsibilities:
- Load configuration and validate structure
- Execute data sources and transformations
- Write output to sinks
"""

import logging
from typing import Dict, Any, List, Tuple

from pyspark.sql import SparkSession, DataFrame

from reader import read_source
from transformers import (
    list_available_transformations,
    create_transformation,
    ValidationError,
    ExecutionError
)
from writer import write_output

logger = logging.getLogger(__name__)


class PipelineEngine:
    """Orchestrate dynamic data transformations based on config."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """Initialize pipeline engine.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary with dataflows definition
        """
        self.spark = spark
        self.config = config
        self.dataframes: Dict[str, DataFrame] = {}
    
    def run(self) -> None:
        """Execute complete pipeline: read → transform → write."""
        flow = self._validate_and_get_flow()
        
        logger.info(f"Starting pipeline with {len(flow.get('sources', []))} source(s)")
        self._execute_sources(flow.get("sources", []))
        
        logger.info(f"Executing {len(flow.get('transformations', []))} transformation(s)")
        self._execute_transformations(flow.get("transformations", []))
        
        logger.info(f"Writing to {len(flow.get('sinks', []))} sink(s)")
        self._execute_sinks(flow.get("sinks", []))
        
        logger.info("✓ Pipeline execution completed")
    
    # =========================================================================
    # VALIDATION
    # =========================================================================
    
    def _validate_and_get_flow(self) -> Dict[str, Any]:
        """Validate config structure and extract dataflow.
        
        Returns:
            First dataflow configuration
            
        Raises:
            TypeError: If config structure is invalid
            ValueError: If required fields are missing
        """
        if not isinstance(self.config, dict):
            raise TypeError("Pipeline config must be a dictionary")
        
        dataflows = self.config.get("dataflows")
        if not isinstance(dataflows, list) or not dataflows:
            raise ValueError("Config must contain a non-empty 'dataflows' list")
        
        flow = dataflows[0]
        if not isinstance(flow, dict):
            raise TypeError("Each dataflow must be a dictionary")
        
        if not flow.get("sources"):
            raise ValueError("Dataflow must include at least one source")
        
        if not flow.get("sinks"):
            raise ValueError("Dataflow must include at least one sink")
        
        logger.info(f"✓ Config validated - dataflow: {flow.get('name', 'unnamed')}")
        return flow
    
    # =========================================================================
    # SOURCE EXECUTION
    # =========================================================================
    
    def _execute_sources(self, sources: List[Dict[str, Any]]) -> None:
        """Load all data sources and register as dataframes.
        
        Args:
            sources: List of source configurations
            
        Raises:
            ValueError: If source is invalid or duplicate name exists
            KeyError: If source cannot be read
        """
        for source in sources:
            source_name = source.get("name")
            
            if not source_name:
                raise ValueError("Each source must define a non-empty 'name'")
            
            if source_name in self.dataframes:
                raise ValueError(f"Duplicate source name detected: '{source_name}'")
            
            try:
                df = read_source(self.spark, source)
                self.dataframes[source_name] = df
                logger.info(f"✓ Loaded source: {source_name} ({df.count()} rows)")
            except Exception as e:
                raise KeyError(f"Failed to load source '{source_name}': {e}") from e
    
    # =========================================================================
    # TRANSFORMATION EXECUTION
    # =========================================================================
    
    def _execute_transformations(self, transformations: List[Dict[str, Any]]) -> None:
        """Apply all transformation steps in order.
        
        Args:
            transformations: List of transformation configurations
            
        Raises:
            KeyError: If input dataframe referenced by transformation doesn't exist
            ValueError: If transformation type is unsupported
            RuntimeError: If transformation execution fails
        """
        for step in transformations:
            step_type = step.get("type")
            step_name = step.get("name", "unnamed")
            params = step.get("params", {})
            input_name = params.get("input")
            
            self._validate_transformation_input(step_name, step_type, input_name)
            self._execute_transformation_step(step_type, step_name, input_name, params)
    
    def _validate_transformation_input(
        self, 
        step_name: str, 
        step_type: str, 
        input_name: str
    ) -> None:
        """Validate that transformation has valid input.
        
        Args:
            step_name: Name of transformation step
            step_type: Type of transformation
            input_name: Name of input dataframe
            
        Raises:
            ValueError: If required fields are missing
            KeyError: If input dataframe doesn't exist
        """
        if not step_type:
            raise ValueError(f"Transformation '{step_name}' must define 'type'")
        
        if not input_name:
            raise ValueError(
                f"Transformation '{step_name}' (type '{step_type}') must define 'params.input'"
            )
        
        if input_name not in self.dataframes:
            raise KeyError(
                f"Transformation '{step_name}' references missing input dataframe '{input_name}'. "
                f"Available: {list(self.dataframes.keys())}"
            )
    
    def _execute_transformation_step(
        self, 
        step_type: str, 
        step_name: str, 
        input_name: str, 
        params: Dict[str, Any]
    ) -> None:
        """Execute single transformation and register result.
        
        Args:
            step_type: Type of transformation
            step_name: Name of transformation step
            input_name: Name of input dataframe
            params: Parameters for transformation
            
        Raises:
            ValueError: If transformation type is unsupported
            RuntimeError: If transformation execution fails
        """
        try:
            handler = create_transformation(
                step_type,
                self.spark,
                self.dataframes[input_name],
                params
            )
            result = handler.execute()
            
            # Special handling for validate_fields (returns 2 dataframes)
            if step_type == "validate_fields":
                ok_df, ko_df = result
                self.dataframes["validation_ok"] = ok_df
                self.dataframes["validation_ko"] = ko_df
                logger.info(
                    f"✓ Transformation '{step_name}' (validate_fields): "
                    f"{ok_df.count()} valid, {ko_df.count()} invalid"
                )
            else:
                if not step_name:
                    raise ValueError(
                        f"Transformation of type '{step_type}' must define a non-empty 'name'"
                    )
                self.dataframes[step_name] = result
                logger.info(
                    f"✓ Transformation '{step_name}' ({step_type}): {result.count()} rows"
                )
        
        except (ValidationError, ExecutionError) as exc:
            raise RuntimeError(
                f"Transformation '{step_name}' (type '{step_type}') failed: {exc}"
            ) from exc
        
        except ValueError as exc:
            # Raised when transformation type is unknown
            available = list_available_transformations()
            raise ValueError(
                f"Unsupported transformation type '{step_type}' in step '{step_name}'. "
                f"Available: {', '.join(sorted(available))}"
            ) from exc
        
        except Exception as exc:
            raise RuntimeError(
                f"Transformation '{step_name}' (type '{step_type}') failed: {exc}"
            ) from exc
    
    # =========================================================================
    # SINK EXECUTION
    # =========================================================================
    
    def _execute_sinks(self, sinks: List[Dict[str, Any]]) -> None:
        """Write dataframes to all configured sinks.
        
        Args:
            sinks: List of sink configurations
            
        Raises:
            KeyError: If sink input dataframe doesn't exist
            RuntimeError: If write operation fails
        """
        for sink in sinks:
            sink_name = sink.get("name", "unnamed")
            sink_input = sink.get("input")
            
            try:
                if sink_input not in self.dataframes:
                    raise KeyError(
                        f"Sink '{sink_name}' references missing input '{sink_input}'. "
                        f"Available: {list(self.dataframes.keys())}"
                    )
                
                write_output(self.dataframes[sink_input], sink)
                logger.info(f"✓ Written sink: {sink_name}")
            
            except Exception as e:
                raise RuntimeError(f"Failed to write sink '{sink_name}': {e}") from e
