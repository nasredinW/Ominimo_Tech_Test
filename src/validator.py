"""
Professional Data Quality Validator

Applies comprehensive validation rules to DataFrames with:
- Multiple validators per field
- Custom error messages
- Professional error reporting
- Data quality metrics
"""

import os
from functools import reduce
from pyspark.sql import functions as F
from typing import Dict, List, Tuple, Any
from pyspark.sql import DataFrame

from validations import ValidatorRegistry, InvalidValidationConfig, ValidatorNotFoundError


# ==============================================================================
# DATA QUALITY VALIDATION ENGINE
# ==============================================================================

class DataQualityValidator:
    """Professional data quality validation engine"""

    @staticmethod
    def _env_truthy(value: str) -> bool:
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _get_int_env(name: str, default: int) -> int:
        raw = os.getenv(name)
        if raw is None or str(raw).strip() == "":
            return default
        try:
            return int(str(raw).strip())
        except ValueError:
            return default
    
    @staticmethod
    def validate(df: DataFrame, rules: List[Dict[str, Any]]) -> Tuple[DataFrame, DataFrame]:
        """
        Apply validation rules to dataframe.
        
        Args:
            df: Input dataframe
            rules: Validation rules list
                [{
                    "field": "age",
                    "validations": [
                        "notNull",
                        {"type": "between", "min": 0, "max": 150}
                    ],
                    "message": "Custom error message"
                }, ...]
        
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        if not isinstance(rules, list):
            raise TypeError("Validation rules must be a list")
        
        if not rules:
            empty_ko = df.limit(0).withColumn("validation_errors", F.lit(None).cast("string"))
            return df, empty_ko
        
        # Build conditions for each field
        field_conditions = {}
        field_errors = {}
        
        for rule in rules:
            field = rule.get("field")
            validators = rule.get("validations", [])
            custom_message = rule.get("message")
            
            if not field:
                raise InvalidValidationConfig("Validation rule missing required 'field'")
            if field not in df.columns:
                raise InvalidValidationConfig(
                    f"Field '{field}' not found in dataframe. Available: {df.columns}"
                )
            if not validators:
                raise InvalidValidationConfig(
                    f"Field '{field}' has no validators defined"
                )
            
            # Build conditions for all validators in this field
            field_validators = []
            field_error_labels = []
            
            for validator_config in validators:
                try:
                    condition = ValidatorRegistry.build_condition(field, validator_config)
                    field_validators.append(condition)
                    
                    # Generate error message: validator message > field message > default
                    if isinstance(validator_config, str):
                        validator_type = validator_config
                        validator_message = None
                    else:
                        validator_type = validator_config.get("type", "unknown")
                        validator_message = validator_config.get("message")
                    
                    error_msg = validator_message or custom_message or f"{field}:{validator_type}"
                    field_error_labels.append(F.when(~condition, F.lit(error_msg)))
                    
                except (ValidatorNotFoundError, InvalidValidationConfig) as e:
                    raise InvalidValidationConfig(
                        f"Error in field '{field}': {str(e)}"
                    )
            
            # Combine all validators for this field (ALL must pass)
            if field_validators:
                combined_condition = reduce(lambda a, b: a & b, field_validators)
                field_conditions[field] = combined_condition
                field_errors[field] = field_error_labels
        
        # Combine all field conditions (ALL fields must pass)
        all_conditions = list(field_conditions.values())
        if not all_conditions:
            empty_ko = df.limit(0).withColumn("validation_errors", F.lit(None).cast("string"))
            return df, empty_ko
        
        final_condition = reduce(lambda a, b: a & b, all_conditions)
        
        # Build error messages
        all_error_labels = []
        for error_list in field_errors.values():
            all_error_labels.extend(error_list)

        # Split into valid and invalid dataframes
        valid_df = df.filter(final_condition)

        # Only compute error messages for invalid rows.
        invalid_df = df.filter(~final_condition)

        # Create a structured array of error strings first; then optionally truncate and stringify.
        invalid_df = invalid_df.withColumn("__validation_errors_raw", F.array(*all_error_labels))
        invalid_df = invalid_df.withColumn(
            "__validation_errors",
            F.expr("filter(__validation_errors_raw, x -> x is not null)")
        )

        max_errors = DataQualityValidator._get_int_env("VALIDATION_MAX_ERRORS_PER_ROW", 50)
        keep_array = DataQualityValidator._env_truthy(os.getenv("VALIDATION_ERRORS_ARRAY", "false"))

        errors_arr_col = F.col("__validation_errors")
        if max_errors > 0:
            limited_arr = F.slice(errors_arr_col, 1, max_errors)
            joined = F.concat_ws("; ", limited_arr)
            more_count = (F.size(errors_arr_col) - F.lit(max_errors)).cast("string")
            errors_str = F.when(
                F.size(errors_arr_col) > max_errors,
                F.concat(joined, F.lit("; ... (+"), more_count, F.lit(" more)")),
            ).otherwise(joined)
        else:
            # max_errors <= 0 means "no truncation"
            errors_str = F.concat_ws("; ", errors_arr_col)

        invalid_df = invalid_df.withColumn(
            "validation_errors",
            F.when(F.size(errors_arr_col) == 0, F.lit("Unknown validation error")).otherwise(errors_str)
        )

        if keep_array:
            invalid_df = invalid_df.withColumn("validation_errors_array", errors_arr_col)

        invalid_df = invalid_df.drop("__validation_errors_raw", "__validation_errors")
        
        return valid_df, invalid_df
    
    @staticmethod
    def get_validation_stats(valid_df: DataFrame, invalid_df: DataFrame) -> Dict[str, Any]:
        """Get validation statistics"""
        # Count each dataframe once to avoid repeated Spark jobs.
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        total = valid_count + invalid_count
        
        return {
            "total_records": total,
            "valid_records": valid_count,
            "invalid_records": invalid_count,
            "valid_percentage": (valid_count / total * 100) if total > 0 else 0,
            "invalid_percentage": (invalid_count / total * 100) if total > 0 else 0,
        }


# ==============================================================================
# BACKWARD COMPATIBLE FUNCTION
# ==============================================================================

def apply_validations(df: DataFrame, rules: List[Dict[str, Any]]) -> Tuple[DataFrame, DataFrame]:
    """
    Apply validations using legacy interface.
    
    This function maintains backward compatibility with existing code
    while using the new professional validation engine.
    
    Args:
        df: Input dataframe
        rules: Validation rules
    
    Returns:
        Tuple of (valid_df, invalid_df)
    """
    return DataQualityValidator.validate(df, rules)