from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
    LongType
)

def get_rules_schema() -> StructType:
    """Returns the PySpark StructType representing the schema for DQ Rules."""
    return StructType([
        StructField("rule_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("filter_condition", StringType(), True),
        StructField("check_type", StringType(), False),
        StructField("check_against_value", StringType(), True),
        StructField("threshold", DoubleType(), False), # e.g. 0.05 for 5% error tolerance
        StructField("is_active", BooleanType(), False),
        StructField("is_mandatory", BooleanType(), True), # Determines if rule failure fails the batch
        StructField("severity", StringType(), True) # warning, error, fatal
    ])

def get_results_schema() -> StructType:
    """Returns the PySpark StructType representing the schema for DQ Results."""
    return StructType([
        StructField("execution_id", StringType(), False),
        StructField("rule_id", StringType(), False),
        StructField("execution_time", TimestampType(), False),
        StructField("total_records", LongType(), False),
        StructField("failed_records", LongType(), False),
        StructField("status", StringType(), False), # PASS, FAIL, ERROR
        StructField("error_message", StringType(), True)
    ])

def create_dq_tables(
    spark, 
    schema_name: str, 
    rules_table_name: str = "dq_rules", 
    results_table_name: str = "dq_results", 
    format: str = "parquet",
    create_schema: bool = False
):
    """
    Creates the rules and results tables in the specified Spark schema if they do not exist.
    Optionally creates the schema if create_schema=True.
    Uses generic formats (parquet/delta).
    """
    if create_schema:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    
    # Create Rules Table
    rules_schema_ddl = get_rules_schema().toDDL()
    rules_table_path = f"{schema_name}.{rules_table_name}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {rules_table_path}
        ({rules_schema_ddl})
        USING {format}
    """)
    
    # Create Results Table
    results_schema_ddl = get_results_schema().toDDL()
    results_table_path = f"{schema_name}.{results_table_name}"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {results_table_path}
        ({results_schema_ddl})
        USING {format}
    """)
    
    print(f"Tables {rules_table_path} and {results_table_path} initialized successfully.")
