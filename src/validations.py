from pyspark.sql.functions import col

def not_empty(column):
    return col(column).isNotNull() & (col(column) != "")

def not_null(column):
    return col(column).isNotNull()

VALIDATION_MAP = {
    "notEmpty": not_empty,
    "notNull": not_null
}