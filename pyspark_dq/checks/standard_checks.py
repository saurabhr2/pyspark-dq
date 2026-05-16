import json
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def _apply_filter(df: DataFrame, rule_dict: dict) -> DataFrame:
    """Applies the filter_condition if it exists in the rule."""
    filter_condition = rule_dict.get("filter_condition")
    if filter_condition:
        return df.filter(F.expr(filter_condition))
    return df

def check_is_null(df: DataFrame, rule_dict: dict) -> dict:
    col_name = rule_dict.get("column_name")
    if not col_name:
        raise ValueError("column_name is required for check_is_not_null")
    
    df_filtered = _apply_filter(df, rule_dict)
    
    total_records = df_filtered.count()
    failed_records = df_filtered.filter(F.col(col_name).isNull()).count()
    
    return {"total_records": total_records, "failed_records": failed_records}

def check_is_unique(df: DataFrame, rule_dict: dict) -> dict:
    col_name = rule_dict.get("column_name")
    if not col_name:
        raise ValueError("column_name is required for check_is_unique")
    
    df_filtered = _apply_filter(df, rule_dict)
    total_records = df_filtered.count()
    
    if total_records == 0:
        return {"total_records": 0, "failed_records": 0}

    distinct_count = df_filtered.select(col_name).distinct().count()
    failed_records = total_records - distinct_count

    return {"total_records": total_records, "failed_records": failed_records}

def check_value_in_range(df: DataFrame, rule_dict: dict) -> dict:
    col_name = rule_dict.get("column_name")
    if not col_name:
        raise ValueError("column_name is required for check_value_in_range")
    
    val_str = rule_dict.get("check_against_value")
    if not val_str:
        raise ValueError("check_against_value (e.g. '[1, 100]') is required for check_value_in_range")
    
    try:
        range_vals = json.loads(val_str)
        min_val, max_val = range_vals[0], range_vals[1]
    except (json.JSONDecodeError, IndexError, TypeError):
        raise ValueError("check_against_value must be a valid JSON array with two elements, e.g., '[1, 100]'")

    df_filtered = _apply_filter(df, rule_dict)
    total_records = df_filtered.count()
    
    # Using 'between' is robust across int, float, double, date, and datetime
    # Failed records are those NOT between min and max, or null
    failed_condition = ~F.col(col_name).between(min_val, max_val) | F.col(col_name).isNull()
    failed_records = df_filtered.filter(failed_condition).count()

    return {"total_records": total_records, "failed_records": failed_records}

def check_is_equal_to(df: DataFrame, rule_dict: dict) -> dict:
    col_name = rule_dict.get("column_name")
    val_str = rule_dict.get("check_against_value")
    if not col_name or val_str is None:
        raise ValueError("column_name and check_against_value are required for is_equal_to")
        
    df_filtered = _apply_filter(df, rule_dict)
    total_records = df_filtered.count()
    failed_records = df_filtered.filter((F.col(col_name) != F.lit(val_str)) | F.col(col_name).isNull()).count()
    return {"total_records": total_records, "failed_records": failed_records}

def check_is_greater_than(df: DataFrame, rule_dict: dict) -> dict:
    col_name = rule_dict.get("column_name")
    val_str = rule_dict.get("check_against_value")
    if not col_name or val_str is None:
        raise ValueError("column_name and check_against_value are required for is_greater_than")
        
    df_filtered = _apply_filter(df, rule_dict)
    total_records = df_filtered.count()
    failed_records = df_filtered.filter((F.col(col_name) <= F.lit(val_str)) | F.col(col_name).isNull()).count()
    return {"total_records": total_records, "failed_records": failed_records}

def check_is_less_than(df: DataFrame, rule_dict: dict) -> dict:
    col_name = rule_dict.get("column_name")
    val_str = rule_dict.get("check_against_value")
    if not col_name or val_str is None:
        raise ValueError("column_name and check_against_value are required for is_less_than")
        
    df_filtered = _apply_filter(df, rule_dict)
    total_records = df_filtered.count()
    failed_records = df_filtered.filter((F.col(col_name) >= F.lit(val_str)) | F.col(col_name).isNull()).count()
    return {"total_records": total_records, "failed_records": failed_records}

def check_date_format(df: DataFrame, rule_dict: dict) -> dict:
    col_name = rule_dict.get("column_name")
    date_format = rule_dict.get("check_against_value")
    if not col_name or not date_format:
        raise ValueError("column_name and check_against_value (format, e.g. 'yyyy-MM-dd') are required for date_format check")
        
    df_filtered = _apply_filter(df, rule_dict)
    total_records = df_filtered.count()
    
    # We apply the format check on non-null values. Nulls should be checked by a null_check if needed.
    not_null_df = df_filtered.filter(F.col(col_name).isNotNull())
    
    # to_date returns null if the string doesn't match the format
    failed_records = not_null_df.filter(F.to_date(F.col(col_name).cast("string"), date_format).isNull()).count()
    
    return {"total_records": total_records, "failed_records": failed_records}

def check_for_duplicates(df: DataFrame, rule_dict: dict) -> dict:
    """Checks for duplicate records based on specified columns or the entire row."""
    col_name = rule_dict.get("column_name")
    
    df_filtered = _apply_filter(df, rule_dict)
    total_records = df_filtered.count()
    
    if total_records == 0:
        return {"total_records": 0, "failed_records": 0}

    # If columns are specified, drop duplicates based on them, else across all columns
    if col_name:
        cols = [c.strip() for c in col_name.split(",")]
        distinct_count = df_filtered.dropDuplicates(cols).count()
    else:
        distinct_count = df_filtered.dropDuplicates().count()
        
    failed_records = total_records - distinct_count

    return {"total_records": total_records, "failed_records": failed_records}

def check_custom_sql(df: DataFrame, rule_dict: dict) -> dict:
    sql_expr = rule_dict.get("check_against_value")
    if not sql_expr:
        raise ValueError("check_against_value (valid SQL expression) is required for check_custom_sql")
    
    df_filtered = _apply_filter(df, rule_dict)
    total_records = df_filtered.count()
    failed_records = df_filtered.filter(~F.expr(sql_expr)).count()

    return {"total_records": total_records, "failed_records": failed_records}

def check_valid_email(df: DataFrame, rule_dict: dict) -> dict:
    col_name = rule_dict.get("column_name")
    if not col_name:
        raise ValueError("column_name is required for valid_email")
    df_filtered = _apply_filter(df, rule_dict)
    total_records = df_filtered.count()
    
    # Simple regex for email
    email_regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    not_null_df = df_filtered.filter(F.col(col_name).isNotNull())
    failed_records = not_null_df.filter(~F.col(col_name).cast("string").rlike(email_regex)).count()
    return {"total_records": total_records, "failed_records": failed_records}

def check_valid_phone(df: DataFrame, rule_dict: dict) -> dict:
    col_name = rule_dict.get("column_name")
    if not col_name:
        raise ValueError("column_name is required for valid_phone")
    # Allow custom regex, else default
    phone_regex = rule_dict.get("check_against_value") or r"^\+?[1-9]\d{1,14}$"
    
    df_filtered = _apply_filter(df, rule_dict)
    total_records = df_filtered.count()
    not_null_df = df_filtered.filter(F.col(col_name).isNotNull())
    failed_records = not_null_df.filter(~F.col(col_name).cast("string").rlike(phone_regex)).count()
    return {"total_records": total_records, "failed_records": failed_records}

def check_valid_pincode(df: DataFrame, rule_dict: dict) -> dict:
    col_name = rule_dict.get("column_name")
    if not col_name:
        raise ValueError("column_name is required for valid_pincode")
    # Allow custom regex for pincode (e.g. US zip, India pincode), default to 5 or 6 digits
    pincode_regex = rule_dict.get("check_against_value") or r"^\d{5,6}$"
    
    df_filtered = _apply_filter(df, rule_dict)
    total_records = df_filtered.count()
    not_null_df = df_filtered.filter(F.col(col_name).isNotNull())
    failed_records = not_null_df.filter(~F.col(col_name).cast("string").rlike(pincode_regex)).count()
    return {"total_records": total_records, "failed_records": failed_records}

def check_is_numeric(df: DataFrame, rule_dict: dict) -> dict:
    col_name = rule_dict.get("column_name")
    if not col_name:
        raise ValueError("column_name is required for is_numeric")
    
    df_filtered = _apply_filter(df, rule_dict)
    total_records = df_filtered.count()
    # Check if cast to double is not null (ignores already nulls if we filter them, but here we count them if they fail cast)
    not_null_df = df_filtered.filter(F.col(col_name).isNotNull())
    failed_records = not_null_df.filter(F.col(col_name).cast("double").isNull()).count()
    return {"total_records": total_records, "failed_records": failed_records}

def check_schema_match(df: DataFrame, rule_dict: dict) -> dict:
    """
    Checks if dataframe matches the expected schema. 
    check_against_value should be a JSON string like: '{"id": "integer", "name": "string"}'
    """
    val_str = rule_dict.get("check_against_value")
    if not val_str:
        raise ValueError("check_against_value (JSON schema) is required for schema_match")
    
    try:
        expected_schema = json.loads(val_str)
    except Exception:
        raise ValueError("check_against_value must be a valid JSON dictionary")
        
    df_filtered = _apply_filter(df, rule_dict)
    total_records = df_filtered.count()
    
    actual_schema = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    
    is_match = True
    for col_name, expected_type in expected_schema.items():
        # simpleString() returns 'int', 'string', 'double', 'date', 'timestamp', etc.
        if col_name not in actual_schema or not actual_schema[col_name].startswith(expected_type):
            is_match = False
            break
            
    # If schema doesn't match, we fail all records in scope
    failed_records = total_records if not is_match else 0
    return {"total_records": total_records, "failed_records": failed_records}

def check_value_in_list(df: DataFrame, rule_dict: dict) -> dict:
    col_name = rule_dict.get("column_name")
    val_str = rule_dict.get("check_against_value")
    if not col_name or not val_str:
        raise ValueError("column_name and check_against_value (JSON list) required for value_in_list")
        
    try:
        valid_values = json.loads(val_str)
    except Exception:
        raise ValueError("check_against_value must be a valid JSON list")
        
    df_filtered = _apply_filter(df, rule_dict)
    total_records = df_filtered.count()
    failed_records = df_filtered.filter(~F.col(col_name).isin(valid_values) | F.col(col_name).isNull()).count()
    return {"total_records": total_records, "failed_records": failed_records}

def check_length_equal(df: DataFrame, rule_dict: dict) -> dict:
    col_name = rule_dict.get("column_name")
    val_str = rule_dict.get("check_against_value")
    if not col_name or not val_str:
        raise ValueError("column_name and check_against_value (int) required for length_equal")
    
    try:
        target_len = int(val_str)
    except ValueError:
        raise ValueError("check_against_value must be an integer for length_equal")
        
    df_filtered = _apply_filter(df, rule_dict)
    total_records = df_filtered.count()
    not_null_df = df_filtered.filter(F.col(col_name).isNotNull())
    failed_records = not_null_df.filter(F.length(F.col(col_name).cast("string")) != target_len).count()
    return {"total_records": total_records, "failed_records": failed_records}

# Registry mapping check types to functions
CHECK_REGISTRY = {
    "is_not_null": check_is_null,
    "is_unique": check_is_unique,
    "value_in_range": check_value_in_range,
    "is_equal_to": check_is_equal_to,
    "is_greater_than": check_is_greater_than,
    "is_less_than": check_is_less_than,
    "date_format": check_date_format,
    "duplicates": check_for_duplicates,
    "valid_email": check_valid_email,
    "valid_phone": check_valid_phone,
    "valid_pincode": check_valid_pincode,
    "is_numeric": check_is_numeric,
    "schema_match": check_schema_match,
    "value_in_list": check_value_in_list,
    "length_equal": check_length_equal,
    "custom_sql": check_custom_sql
}