from functools import reduce

from pyspark.sql.functions import concat_ws, lit, when

from validations import VALIDATION_MAP


def apply_validations(df, rules):
    if not isinstance(rules, list):
        raise TypeError("Validation rules must be a list")

    if not rules:
        empty_ko = df.limit(0).withColumn("validation_errors", lit(None).cast("string"))
        return df, empty_ko

    conditions = []
    error_labels = []

    for rule in rules:
        field = rule.get("field")
        validators = rule.get("validations", [])

        if not field:
            raise ValueError("Validation rule is missing required 'field'")
        if field not in df.columns:
            raise ValueError(f"Validation field '{field}' not found in dataframe columns: {df.columns}")
        if not validators:
            raise ValueError(f"Validation rule for field '{field}' must include at least one validator")

        for validator_name in validators:
            func = VALIDATION_MAP.get(validator_name)
            if not func:
                raise ValueError(
                    f"Unsupported validator '{validator_name}' for field '{field}'. "
                    f"Supported values: {sorted(VALIDATION_MAP.keys())}"
                )

            cond = func(df, field)
            conditions.append(cond)
            error_labels.append(when(~cond, lit(f"{field}:{validator_name}")))

    final_condition = reduce(lambda left, right: left & right, conditions)

    ok_df = df.filter(final_condition)
    ko_df = df.filter(~final_condition).withColumn("validation_errors", concat_ws("; ", *error_labels))

    return ok_df, ko_df