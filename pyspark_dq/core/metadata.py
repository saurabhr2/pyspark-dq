from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row
from pyspark_dq.utils.setup import get_rules_schema

def load_active_rules_from_df(rules_df: DataFrame, table_name: str) -> List[Dict]:
    """
    Loads active rules for a given table from a PySpark DataFrame into a list of dictionaries.
    """
    active_rules = rules_df.filter(
        (rules_df.table_name == table_name) & 
        (rules_df.is_active == True)
    ).collect()
    
    return [row.asDict() for row in active_rules]

def load_active_rules_from_list(rules_list: List[Dict], table_name: str) -> List[Dict]:
    """
    Filters a runtime list of rule dictionaries for active rules matching the table name.
    """
    return [
        rule for rule in rules_list 
        if rule.get("table_name") == table_name and rule.get("is_active") is True
    ]

def save_rules(spark: SparkSession, rules_list: List[Dict], rules_table_path: str) -> DataFrame:
    """
    Saves a list of rule dictionaries to the specified rules table.
    """
    if not rules_list:
        return spark.createDataFrame([], get_rules_schema())
        
    rows = []
    for r in rules_list:
        rows.append(Row(
            rule_id=r.get("rule_id", ""),
            table_name=r.get("table_name", ""),
            column_name=r.get("column_name", None),
            filter_condition=r.get("filter_condition", None),
            check_type=r.get("check_type", ""),
            check_against_value=r.get("check_against_value", None),
            threshold=r.get("threshold", 0.0),
            is_active=r.get("is_active", True),
            is_mandatory=r.get("is_mandatory", False),
            severity=r.get("severity", None)
        ))
        
    rules_df = spark.createDataFrame(rows, schema=get_rules_schema())
    rules_df.write.mode("append").insertInto(rules_table_path)
    return rules_df
