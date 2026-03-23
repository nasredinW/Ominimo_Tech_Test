"""
Transformation Handler Registry

This module registers all available transformation handlers.
New transformation types can be added here without modifying the engine.
"""

from pyspark.sql.functions import current_timestamp, col


class TransformationHandler:
    """Base class for transformation handlers"""
    
    def __init__(self, spark, df, params):
        self.spark = spark
        self.df = df
        self.params = params
    
    def execute(self):
        raise NotImplementedError("Subclasses must implement execute()")


class ValidateFieldsHandler(TransformationHandler):
    """Handles field validation transformations"""
    
    def execute(self):
        from validator import apply_validations
        validations = self.params.get("validations", [])
        return apply_validations(self.df, validations)


class AddFieldsHandler(TransformationHandler):
    """Handles adding computed fields"""
    
    def execute(self):
        fields = self.params.get("addFields", [])
        df = self.df
        
        for field in fields:
            field_name = field.get("name")
            function_name = field.get("function")
            
            if not field_name:
                raise ValueError("Each add_fields entry must include a non-empty 'name'")
            if not function_name:
                raise ValueError(f"Transformation field '{field_name}' is missing 'function'")
            
            if function_name == "current_timestamp":
                df = df.withColumn(field_name, current_timestamp())
            else:
                raise ValueError(f"Unsupported add_fields function '{function_name}' for field '{field_name}'")
        
        return df


class FilterRowsHandler(TransformationHandler):
    """Handles row filtering with SQL conditions"""
    
    def execute(self):
        condition = self.params.get("condition")
        if not condition:
            raise ValueError("Filter transformation is missing required 'condition'")
        
        try:
            return self.df.filter(condition)
        except Exception as exc:
            raise RuntimeError(f"Failed to apply filter condition '{condition}': {exc}") from exc


class DeriveColumnHandler(TransformationHandler):
    """Handles derived columns with SQL expressions"""
    
    def execute(self):
        column_name = self.params.get("column")
        expression = self.params.get("expression")
        
        if not column_name:
            raise ValueError("Derive column transformation is missing required 'column'")
        if not expression:
            raise ValueError("Derive column transformation is missing required 'expression'")
        
        try:
            return self.df.selectExpr("*", f"{expression} AS {column_name}")
        except Exception as exc:
            raise RuntimeError(f"Failed to derive column '{column_name}' with expression '{expression}': {exc}") from exc


class SelectColumnsHandler(TransformationHandler):
    """Handles column selection and projection"""
    
    def execute(self):
        columns = self.params.get("columns", [])
        if not columns:
            raise ValueError("Select columns transformation is missing required 'columns'")
        
        try:
            return self.df.select(columns)
        except Exception as exc:
            raise RuntimeError(f"Failed to select columns {columns}: {exc}") from exc


class RenameColumnsHandler(TransformationHandler):
    """Handles column renaming"""
    
    def execute(self):
        mappings = self.params.get("mappings", {})
        if not mappings:
            raise ValueError("Rename columns transformation is missing required 'mappings'")
        
        df = self.df
        try:
            for old_name, new_name in mappings.items():
                df = df.withColumnRenamed(old_name, new_name)
            return df
        except Exception as exc:
            raise RuntimeError(f"Failed to rename columns {mappings}: {exc}") from exc


class DropColumnsHandler(TransformationHandler):
    """Handles column dropping"""
    
    def execute(self):
        columns = self.params.get("columns", [])
        if not columns:
            raise ValueError("Drop columns transformation is missing required 'columns'")
        
        try:
            return self.df.drop(*columns)
        except Exception as exc:
            raise RuntimeError(f"Failed to drop columns {columns}: {exc}") from exc


# Registry mapping transformation types to their handler classes
TRANSFORMATION_REGISTRY = {
    "validate_fields": ValidateFieldsHandler,
    "add_fields": AddFieldsHandler,
    "filter_rows": FilterRowsHandler,
    "derive_column": DeriveColumnHandler,
    "select_columns": SelectColumnsHandler,
    "rename_columns": RenameColumnsHandler,
    "drop_columns": DropColumnsHandler,
}


def get_transformation_handler(transformation_type):
    """
    Get the handler class for a transformation type.
    
    Args:
        transformation_type: Type of transformation (e.g., "filter_rows")
    
    Returns:
        Handler class or None if not found
    """
    return TRANSFORMATION_REGISTRY.get(transformation_type)


def list_available_transformations():
    """
    List all available transformation types.
    
    Returns:
        List of available transformation type names
    """
    return list(TRANSFORMATION_REGISTRY.keys())
