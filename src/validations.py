"""
Professional Data Quality Validation Framework (v2)

Supports comprehensive validation types without code changes:

SIMPLE VALIDATORS (no parameters):
  - notNull: Check field is not null
  - notEmpty: Check field is not null and not empty string
  - numeric: Check field is numeric
  - integer: Check field is integer
  - positive: Check field > 0
  - nonNegative: Check field >= 0
  - email: Validate email format
  - phone: Validate phone number
  - dateFormat: Validate ISO date format (YYYY-MM-DD)
  - uuid: Validate UUID format

PARAMETERIZED VALIDATORS (require parameters):
  - minLength: Minimum string length
  - maxLength: Maximum string length
  - between: Value between min and max
  - pattern: Match regex pattern
  - inList: Value in allowed list
  - equals: Exact match value
  - startsWith: String starts with value
  - endsWith: String ends with value
  - contains: String contains value

ADVANCED VALIDATORS:
  - custom_sql: Execute custom SQL condition
  - custom_expr: Use custom expression with column reference

Configuration Example:
  {
    "field": "age",
    "validations": [
      {"type": "notNull", "message": "Age is required"},
      {"type": "between", "min": 0, "max": 150, "message": "Age must be 0-150"},
      {"type": "integer", "message": "Age must be integer"}
    ]
  }
"""

from pyspark.sql.functions import (
    col, when, length, regexp_replace, abs as spark_abs,
    to_date, to_timestamp, year, month, dayofmonth
)
import re
from typing import Dict, List, Any, Callable, Optional, Tuple


# ==============================================================================
#  VALIDATION ERROR CLASS
# ==============================================================================

class ValidationException(Exception):
    """Base exception for validation errors"""
    pass


class ValidatorNotFoundError(ValidationException):
    """Raised when validator type is not found"""
    pass


class InvalidValidationConfig(ValidationException):
    """Raised when validation configuration is invalid"""
    pass


# ==============================================================================
# VALIDATION BUILDERS (Pure Functions)
# ==============================================================================

class ValidationBuilders:
    """
    Pure functions for building validation conditions.
    Each function returns a Spark SQL condition.
    """
    
    @staticmethod
    def not_null(column: str) -> Any:
        """Column is not null"""
        return col(column).isNotNull()
    
    @staticmethod
    def not_empty(column: str) -> Any:
        """Column is not null and not empty string"""
        return col(column).isNotNull() & (col(column) != "")
    
    @staticmethod
    def numeric(column: str) -> Any:
        """Column can be cast to numeric"""
        try:
            return col(column).cast("double").isNotNull()
        except:
            return col(column).rlike(r"^-?\d+(\.\d+)?$")
    
    @staticmethod
    def integer(column: str) -> Any:
        """Column is integer"""
        return col(column).rlike(r"^-?\d+$")
    
    @staticmethod
    def positive(column: str) -> Any:
        """Column > 0"""
        return col(column).cast("double") > 0
    
    @staticmethod
    def non_negative(column: str) -> Any:
        """Column >= 0"""
        return col(column).cast("double") >= 0
    
    @staticmethod
    def email(column: str) -> Any:
        """Email format validation"""
        pattern = r"^[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)*[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$"
        return col(column).rlike(pattern)
    
    @staticmethod
    def phone(column: str) -> Any:
        """Phone number validation (international format)"""
        pattern = r"^\+?[1-9]\d{1,14}$"
        return col(column).rlike(pattern)
    
    @staticmethod
    def date_format(column: str) -> Any:
        """ISO 8601 date format (YYYY-MM-DD)"""
        pattern = r"^\d{4}-\d{2}-\d{2}$"
        return col(column).rlike(pattern)
    
    @staticmethod
    def datetime_format(column: str) -> Any:
        """ISO 8601 datetime format (YYYY-MM-DDTHH:MM:SS)"""
        pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
        return col(column).rlike(pattern)
    
    @staticmethod
    def uuid(column: str) -> Any:
        """UUID format validation"""
        pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        return col(column).rlike(pattern)
    
    @staticmethod
    def url(column: str) -> Any:
        """URL format validation"""
        pattern = r"^https?://[^\s/$.?#].[^\s]*$"
        return col(column).rlike(pattern)
    
    @staticmethod
    def min_length(column: str, value: int = None, min_len: int = None) -> Any:
        """Minimum string length"""
        min_value = min_len or value
        if not min_value:
            raise ValueError("minLength requires 'value' or 'min_len' parameter")
        return length(col(column)) >= min_value
    
    @staticmethod
    def max_length(column: str, value: int = None, max_len: int = None) -> Any:
        """Maximum string length"""
        max_value = max_len or value
        if not max_value:
            raise ValueError("maxLength requires 'value' or 'max_len' parameter")
        return length(col(column)) <= max_value
    
    @staticmethod
    def length_between(column: str, min_len: int, max_len: int) -> Any:
        """String length between min and max"""
        return (length(col(column)) >= min_len) & (length(col(column)) <= max_len)
    
    @staticmethod
    def between(column: str, min: Any = None, max: Any = None, min_val: Any = None, max_val: Any = None) -> Any:
        """Value between min and max (inclusive)"""
        min_value = min if min is not None else min_val
        max_value = max if max is not None else max_val
        if min_value is None or max_value is None:
            raise ValueError("between requires 'min' and 'max' parameters")
        return (col(column).cast("double") >= min_value) & (col(column).cast("double") <= max_value)
    
    @staticmethod
    def greater_than(column: str, value: Any) -> Any:
        """Column > value"""
        return col(column).cast("double") > value
    
    @staticmethod
    def less_than(column: str, value: Any) -> Any:
        """Column < value"""
        return col(column).cast("double") < value
    
    @staticmethod
    def equals(column: str, value: Any) -> Any:
        """Column equals value"""
        return col(column) == value
    
    @staticmethod
    def pattern(column: str, regex_pattern: str = None, regex: str = None) -> Any:
        """Column matches regex pattern"""
        pattern_val = regex or regex_pattern
        if not pattern_val:
            raise ValueError("pattern requires 'regex' or 'regex_pattern' parameter")
        return col(column).rlike(pattern_val)
    
    @staticmethod
    def in_list(column: str, allowed_values: List[Any] = None, values: List[Any] = None) -> Any:
        """Column value in allowed list"""
        list_vals = values or allowed_values
        if not list_vals:
            raise ValueError("inList requires 'values' or 'allowed_values' parameter")
        return col(column).isin(list_vals)
    
    @staticmethod
    def starts_with(column: str, prefix: str) -> Any:
        """String starts with prefix"""
        return col(column).startswith(prefix)
    
    @staticmethod
    def ends_with(column: str, suffix: str) -> Any:
        """String ends with suffix"""
        return col(column).endswith(suffix)
    
    @staticmethod
    def contains(column: str, substring: str) -> Any:
        """String contains substring"""
        return col(column).contains(substring)
    
    @staticmethod
    def unique(column: str) -> Any:
        """Marker for uniqueness check (handled in ValidatorRegistry)"""
        return col(column).isNotNull()


# ==============================================================================
#  VALIDATOR REGISTRY
# ==============================================================================

class ValidatorRegistry:
    """
    Professional registry for all validation types.
    Supports simple, parameterized, and custom validators.
    """
    
    # Simple validators (no parameters)
    _simple_validators = {
        "notNull": ValidationBuilders.not_null,
        "notEmpty": ValidationBuilders.not_empty,
        "numeric": ValidationBuilders.numeric,
        "integer": ValidationBuilders.integer,
        "positive": ValidationBuilders.positive,
        "nonNegative": ValidationBuilders.non_negative,
        "email": ValidationBuilders.email,
        "phone": ValidationBuilders.phone,
        "dateFormat": ValidationBuilders.date_format,
        "datetimeFormat": ValidationBuilders.datetime_format,
        "uuid": ValidationBuilders.uuid,
        "url": ValidationBuilders.url,
    }
    
    # Parameterized validators
    _param_validators = {
        "minLength": ValidationBuilders.min_length,
        "maxLength": ValidationBuilders.max_length,
        "lengthBetween": ValidationBuilders.length_between,
        "between": ValidationBuilders.between,
        "greaterThan": ValidationBuilders.greater_than,
        "lessThan": ValidationBuilders.less_than,
        "equals": ValidationBuilders.equals,
        "pattern": ValidationBuilders.pattern,
        "inList": ValidationBuilders.in_list,
        "startsWith": ValidationBuilders.starts_with,
        "endsWith": ValidationBuilders.ends_with,
        "contains": ValidationBuilders.contains,
    }
    
    @classmethod
    def build_condition(cls, column: str, validator_config: Dict[str, Any]) -> Any:
        """
        Build validation condition from config.
        
        Args:
            column: Column name
            validator_config: Validator configuration dict
                - If string: simple validator name
                - If dict: {"type": "validator_name", "param1": value1, ...}
        
        Returns:
            Spark SQL condition
        
        Raises:
            ValidatorNotFoundError: If validator not found
            InvalidValidationConfig: If config is invalid
        """
        # Metadata keys to exclude from validator parameters
        METADATA_KEYS = {"type", "message", "enabled", "description"}
        
        # Handle string format (backward compatibility)
        if isinstance(validator_config, str):
            validator_type = validator_config
            params = {}
        elif isinstance(validator_config, dict):
            validator_type = validator_config.get("type")
            # Extract only validator parameters, exclude metadata keys
            params = {k: v for k, v in validator_config.items() if k not in METADATA_KEYS}
        else:
            raise InvalidValidationConfig(
                f"Invalid validator config: {validator_config}. Must be string or dict."
            )
        
        if not validator_type:
            raise InvalidValidationConfig("Validator type not specified")
        
        # Try simple validators first
        if validator_type in cls._simple_validators:
            return cls._simple_validators[validator_type](column)
        
        # Try parameterized validators
        if validator_type in cls._param_validators:
            if not params:
                raise InvalidValidationConfig(
                    f"Validator '{validator_type}' requires parameters: {cls._get_validator_params(validator_type)}"
                )
            
            return cls._param_validators[validator_type](column, **params)
        
        # Handle custom SQL expression
        if validator_type == "custom_sql":
            expr = params.get("expression")
            if not expr:
                raise InvalidValidationConfig("custom_sql requires 'expression' parameter")
            # Replace {column} placeholder with actual column reference
            expr = expr.replace("{column}", f"`{column}`")
            from pyspark.sql import functions as F
            return F.expr(expr)
        
        # Unknown validator
        available = cls.list_all()
        raise ValidatorNotFoundError(
            f"Unknown validator '{validator_type}'. Available: {', '.join(available)}"
        )
    
    @classmethod
    def list_all(cls) -> List[str]:
        """List all available validators"""
        return sorted(list(cls._simple_validators.keys()) + list(cls._param_validators.keys()) + ["custom_sql"])
    
    @classmethod
    def _get_validator_params(cls, validator_type: str) -> List[str]:
        """Get parameters for a specific validator"""
        # Simple heuristic for documentation
        param_hints = {
            "minLength": ["value"],
            "maxLength": ["value"],
            "lengthBetween": ["min_len", "max_len"],
            "between": ["min", "max"],
            "greaterThan": ["value"],
            "lessThan": ["value"],
            "equals": ["value"],
            "pattern": ["regex"],
            "inList": ["values"],
            "startsWith": ["prefix"],
            "endsWith": ["suffix"],
            "contains": ["substring"],
        }
        return param_hints.get(validator_type, ["parameters"])


# ==============================================================================
# VALIDATION MAP (For backward compatibility with old validator.py)
# ==============================================================================

VALIDATION_MAP = {
    "notEmpty": lambda col: ValidationBuilders.not_empty(col),
    "notNull": lambda col: ValidationBuilders.not_null(col),
    "numeric": lambda col: ValidationBuilders.numeric(col),
    "positive": lambda col: ValidationBuilders.positive(col),
    "nonNegative": lambda col: ValidationBuilders.non_negative(col),
    "email": lambda col: ValidationBuilders.email(col),
    "phone": lambda col: ValidationBuilders.phone(col),
    "dateFormat": lambda col: ValidationBuilders.date_format(col),
}