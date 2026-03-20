from pyspark.sql.functions import current_timestamp


def apply_transformations(df, fields):
    if not fields:
        return df

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