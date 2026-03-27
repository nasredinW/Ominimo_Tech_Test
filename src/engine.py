from reader import read_source
from transformers_registry_v2 import (
    get_transformation_handler,
    list_available_transformations,
    create_transformation,
    ValidationError,
    ExecutionError
)
from writer import write_output


class PipelineEngine:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.dataframes = {}

    def run(self):
        flow = self._get_flow()

        # 1. Read sources
        for source in flow.get("sources", []):
            source_name = source.get("name")
            if not source_name:
                raise ValueError("Each source must define a non-empty 'name'")
            if source_name in self.dataframes:
                raise ValueError(f"Duplicate source or dataframe name detected: '{source_name}'")

            df = read_source(self.spark, source)
            self.dataframes[source_name] = df

        # 2. Transformations (dynamically dispatched via registry)
        for step in flow.get("transformations", []):
            step_type = step.get("type")
            step_name = step.get("name", "unnamed")
            params = step.get("params", {})
            input_name = params.get("input")

            if input_name not in self.dataframes:
                raise KeyError(
                    f"Transformation '{step_name}' references missing input dataframe '{input_name}'"
                )

            # Create and execute the transformation using the v2 registry
            try:
                handler = create_transformation(
                    step_type,
                    self.spark,
                    self.dataframes[input_name],
                    params
                )
                result = handler.execute()
                
                # Special handling for validate_fields which returns two dataframes
                if step_type == "validate_fields":
                    ok_df, ko_df = result
                    self.dataframes["validation_ok"] = ok_df
                    self.dataframes["validation_ko"] = ko_df
                else:
                    # Store the result under the transformation's name
                    if not step_name:
                        raise ValueError(f"Transformation of type '{step_type}' must define a non-empty 'name'")
                    self.dataframes[step_name] = result
            except (ValidationError, ExecutionError) as exc:
                raise RuntimeError(f"Transformation '{step_name}' (type '{step_type}') failed: {exc}") from exc
            except ValueError as exc:
                # Raised when transformation type is unknown
                available = list_available_transformations()
                raise ValueError(
                    f"Unsupported transformation type '{step_type}' in step '{step_name}'. "
                    f"Available types: {', '.join(available)}"
                ) from exc
            except Exception as exc:
                raise RuntimeError(f"Transformation '{step_name}' (type '{step_type}') failed: {exc}") from exc

        # 3. Write outputs
        for sink in flow.get("sinks", []):
            sink_input = sink.get("input")
            if sink_input not in self.dataframes:
                raise KeyError(f"Sink '{sink.get('name', 'unnamed')}' references missing input '{sink_input}'")
            write_output(self.dataframes[sink_input], sink)

    def _get_flow(self):
        if not isinstance(self.config, dict):
            raise TypeError("Pipeline config must be a dictionary")

        dataflows = self.config.get("dataflows")
        if not isinstance(dataflows, list) or not dataflows:
            raise ValueError("Config must contain a non-empty 'dataflows' list")

        flow = dataflows[0]
        if not isinstance(flow, dict):
            raise TypeError("Each dataflow must be an object")

        if not flow.get("sources"):
            raise ValueError("Dataflow must include at least one source")

        if not flow.get("sinks"):
            raise ValueError("Dataflow must include at least one sink")

        return flow