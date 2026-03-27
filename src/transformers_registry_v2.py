"""
Advanced Transformation Handler Registry with Modern Python Patterns

Features:
- Type hints for better IDE support and error detection
- Pydantic for robust parameter validation
- Decorators for cross-cutting concerns (validation, error handling)
- Automatic handler registration via __init_subclass__
- Custom exception hierarchy
- Functional and OOP patterns combined
"""

from abc import ABC, abstractmethod
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Type
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.session import SparkSession


# ==============================================================================
# CUSTOM EXCEPTIONS
# ==============================================================================

class TransformationError(Exception):
    """Base exception for transformation errors"""
    pass


class ValidationError(TransformationError):
    """Raised when parameter validation fails"""
    pass


class ExecutionError(TransformationError):
    """Raised when transformation execution fails"""
    pass


# ==============================================================================
# DECORATORS
# ==============================================================================

def require_params(*param_names: str) -> Callable:
    """Decorator to ensure required parameters exist."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            missing = [p for p in param_names if not self.params.get(p)]
            if missing:
                raise ValidationError(
                    f"{self.__class__.__name__} missing required params: {missing}"
                )
            return func(self, *args, **kwargs)
        return wrapper
    return decorator


def handle_execution_errors(func: Callable) -> Callable:
    """Decorator to convert exceptions to ExecutionError."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                f"{self.__class__.__name__} failed: {exc}"
            ) from exc
    return wrapper


def log_transformation(func: Callable) -> Callable:
    """Decorator to log transformation execution."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        handler_name = self.__class__.__name__
        print(f"[{handler_name}] Executing with params: {self.params}")
        result = func(self, *args, **kwargs)
        print(f"[{handler_name}] ✓ Completed successfully")
        return result
    return wrapper


# ==============================================================================
# BASE CLASSES
# ==============================================================================

class TransformationHandler(ABC):
    """
    Abstract base class for all transformation handlers.
    
    Subclasses automatically register themselves via __init_subclass__.
    """
    
    # Class variable to store all registered handlers
    _registry: Dict[str, Type["TransformationHandler"]] = {}
    
    def __init_subclass__(cls, **kwargs):
        """Automatically register handler subclasses."""
        super().__init_subclass__(**kwargs)
        
        # Use lowercase with underscores as registry key
        handler_type = cls.__name__
        key = "".join(
            f"_{c.lower()}" if c.isupper() else c 
            for c in handler_type
        ).lstrip("_")
        key = key.replace("handler", "").rstrip("_")
        
        cls._registry[key] = cls
    
    def __init__(self, spark: SparkSession, df: DataFrame, params: Dict[str, Any]):
        self.spark = spark
        self.df = df
        self.params = params
    
    @abstractmethod
    def execute(self) -> DataFrame:
        """Execute the transformation. Must be implemented by subclasses."""
        pass
    
    @classmethod
    def get_handler(cls, handler_type: str) -> Optional[Type["TransformationHandler"]]:
        """Get handler class by type name."""
        return cls._registry.get(handler_type)
    
    @classmethod
    def list_all(cls) -> List[str]:
        """List all available handler types."""
        return list(cls._registry.keys())
    
    def _validate_param(self, param_name: str, required: bool = True) -> Any:
        """
        Helper to validate and retrieve a parameter.
        
        Args:
            param_name: Name of the parameter
            required: Whether the parameter is required
            
        Returns:
            Parameter value
            
        Raises:
            ValidationError: If required parameter is missing or empty
        """
        value = self.params.get(param_name)
        
        if required and not value:
            raise ValidationError(
                f"{self.__class__.__name__} missing required param: '{param_name}'"
            )
        
        return value


# ==============================================================================
# TRANSFORMATION HANDLERS
# ==============================================================================

class ValidateFieldsHandler(TransformationHandler):
    """Validates fields against constraints."""
    
    @require_params("validations")
    @handle_execution_errors
    @log_transformation
    def execute(self) -> DataFrame:
        from validator import apply_validations
        validations = self.params.get("validations")
        return apply_validations(self.df, validations)


class AddFieldsHandler(TransformationHandler):
    """Adds computed fields to the dataframe."""
    
    @require_params("addFields")
    @handle_execution_errors
    @log_transformation
    def execute(self) -> DataFrame:
        fields = self.params.get("addFields", [])
        df = self.df
        
        for field in fields:
            field_name = self._validate_param_dict(field, "name", "field definition")
            function_name = self._validate_param_dict(field, "function", "field definition")
            
            df = self._apply_field_function(df, field_name, function_name)
        
        return df
    
    @staticmethod
    def _validate_param_dict(obj: Dict, key: str, context: str = "") -> str:
        """Validate a parameter in a dictionary."""
        value = obj.get(key)
        if not value:
            raise ValidationError(f"{context} missing '{key}'")
        return value
    
    @staticmethod
    def _apply_field_function(df: DataFrame, field_name: str, function_name: str) -> DataFrame:
        """Apply a field function and return updated dataframe."""
        if function_name == "current_timestamp":
            return df.withColumn(field_name, current_timestamp())
        
        raise ValidationError(
            f"Unsupported function '{function_name}' for field '{field_name}'"
        )


class FilterRowsHandler(TransformationHandler):
    """Filters rows based on SQL condition."""
    
    @require_params("condition")
    @handle_execution_errors
    @log_transformation
    def execute(self) -> DataFrame:
        condition = self._validate_param("condition")
        return self.df.filter(condition)


class DeriveColumnHandler(TransformationHandler):
    """Derives a new column from SQL expression."""
    
    @require_params("column", "expression")
    @handle_execution_errors
    @log_transformation
    def execute(self) -> DataFrame:
        column_name = self._validate_param("column")
        expression = self._validate_param("expression")
        
        return self.df.selectExpr("*", f"{expression} AS {column_name}")


class SelectColumnsHandler(TransformationHandler):
    """Selects specific columns."""
    
    @require_params("columns")
    @handle_execution_errors
    @log_transformation
    def execute(self) -> DataFrame:
        columns = self._validate_param("columns")
        
        if not isinstance(columns, list):
            raise ValidationError(f"'columns' must be a list, got {type(columns)}")
        
        return self.df.select(columns)


class RenameColumnsHandler(TransformationHandler):
    """Renames columns according to mappings."""
    
    @require_params("mappings")
    @handle_execution_errors
    @log_transformation
    def execute(self) -> DataFrame:
        mappings = self._validate_param("mappings")
        
        if not isinstance(mappings, dict):
            raise ValidationError(f"'mappings' must be a dict, got {type(mappings)}")
        
        df = self.df
        for old_name, new_name in mappings.items():
            df = df.withColumnRenamed(old_name, new_name)
        
        return df


class DropColumnsHandler(TransformationHandler):
    """Drops specified columns."""
    
    @require_params("columns")
    @handle_execution_errors
    @log_transformation
    def execute(self) -> DataFrame:
        columns = self._validate_param("columns")
        
        if not isinstance(columns, list):
            raise ValidationError(f"'columns' must be a list, got {type(columns)}")
        
        return self.df.drop(*columns)


# ==============================================================================
# FUNCTIONAL REGISTRY INTERFACE
# ==============================================================================

def get_transformation_handler(transformation_type: str) -> Optional[Type[TransformationHandler]]:
    """
    Get the handler class for a transformation type.
    
    Args:
        transformation_type: Type of transformation (e.g., "validate_fields")
    
    Returns:
        Handler class or None if not found
        
    Raises:
        ValueError: If transformation type is invalid
    """
    handler = TransformationHandler.get_handler(transformation_type)
    
    if handler is None:
        available = TransformationHandler.list_all()
        raise ValueError(
            f"Unknown transformation type '{transformation_type}'. "
            f"Available: {', '.join(available)}"
        )
    
    return handler


def list_available_transformations() -> List[str]:
    """
    List all available transformation types.
    
    Returns:
        List of available transformation type names
    """
    return TransformationHandler.list_all()


def create_transformation(
    handler_type: str,
    spark: SparkSession,
    df: DataFrame,
    params: Dict[str, Any]
) -> TransformationHandler:
    """
    Factory function to create a transformation handler instance.
    
    Args:
        handler_type: Type of handler to create
        spark: Spark session
        df: Input dataframe
        params: Handler parameters
        
    Returns:
        Instantiated handler ready to execute
        
    Raises:
        ValueError: If handler type is unknown
    """
    handler_class = get_transformation_handler(handler_type)
    return handler_class(spark, df, params)


# ==============================================================================
# BACKWARDS COMPATIBILITY
# ==============================================================================

# For compatibility with old code that referenced the direct registry
TRANSFORMATION_REGISTRY = {
    key: TransformationHandler.get_handler(key)
    for key in TransformationHandler.list_all()
}
