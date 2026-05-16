import datetime
from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row
from pyspark_dq.utils.setup import get_results_schema

def save_results(spark: SparkSession, results_list: List[Dict], results_table_path: str = None) -> DataFrame:
    """
    Converts a list of result dictionaries into a PySpark DataFrame.
    If results_table_path is provided, appends the results to that table.
    """
    if not results_list:
        return spark.createDataFrame([], get_results_schema())
        
    # Ensure all required fields are present according to schema
    rows = []
    for r in results_list:
        rows.append(Row(
            execution_id=r.get("execution_id", ""),
            rule_id=r.get("rule_id", ""),
            execution_time=r.get("execution_time", datetime.datetime.now()),
            total_records=r.get("total_records", 0),
            failed_records=r.get("failed_records", 0),
            status=r.get("status", "ERROR"),
            error_message=r.get("error_message", None)
        ))
        
    results_df = spark.createDataFrame(rows, schema=get_results_schema())
    
    if results_table_path:
        results_df.write.mode("append").insertInto(results_table_path)
        
    return results_df
