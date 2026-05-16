import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark_dq.core.engine import DQEngine
from pyspark_dq.core.metadata import load_active_rules_from_list

def test_dq_engine_with_filters(spark):
    # Sample Data
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    
    data = [
        (1, "Alice", "ACTIVE", 30),
        (2, None, "ACTIVE", 25),    # name is null
        (3, "Bob", "INACTIVE", 40),
        (4, "Charlie", "ACTIVE", 15), # age out of range
        (5, "David", "ACTIVE", 15),   # duplicate age
        (6, "Alice", "ACTIVE", 30),   # complete duplicate
    ]
    
    df = spark.createDataFrame(data, schema)
    
    rules = [
        {
            "rule_id": "r1",
            "table_name": "users",
            "column_name": "name",
            "check_type": "is_not_null",
            "threshold": 0.0,
            "is_active": True,
            "filter_condition": "status = 'ACTIVE'" # Only check active users
        },
        {
            "rule_id": "r2",
            "table_name": "users",
            "column_name": "age",
            "check_type": "value_in_range",
            "check_against_value": "[18, 99]",
            "threshold": 0.0,
            "is_active": True,
            "filter_condition": "status = 'ACTIVE'"
        },
        {
            "rule_id": "r3",
            "table_name": "users",
            "column_name": None,
            "check_type": "custom_sql",
            "check_against_value": "age >= 18 OR status = 'INACTIVE'",
            "threshold": 0.0,
            "is_active": True
        }
    ]
    
    active_rules = load_active_rules_from_list(rules, "users")
    
    engine = DQEngine(spark)
    execution_output = engine.execute_rules(df, active_rules)
    
    results = execution_output["results"]
    batch_status = execution_output["batch_status"]
    
    assert len(results) == 3
    # None of the rules are marked as mandatory, so batch_status should be PASS despite failures
    assert batch_status == "PASS"
    
    # r1: Name should not be null for ACTIVE users. Total active = 5. One has null name (id 2).
    r1_res = next(r for r in results if r["rule_id"] == "r1")
    assert r1_res["total_records"] == 5
    assert r1_res["failed_records"] == 1
    assert r1_res["status"] == "FAIL"
    
    # r2: Age between 18 and 99 for ACTIVE users. Total active = 5. Two are 15 (id 4, 5).
    r2_res = next(r for r in results if r["rule_id"] == "r2")
    assert r2_res["total_records"] == 5
    assert r2_res["failed_records"] == 2
    assert r2_res["status"] == "FAIL"
    
    # r3: Custom SQL (age >= 18 OR status = 'INACTIVE') for all users. Total = 6.
    # Failed ones: id 4 (15, ACTIVE), id 5 (15, ACTIVE) -> 2 failures.
    r3_res = next(r for r in results if r["rule_id"] == "r3")
    assert r3_res["total_records"] == 6
    assert r3_res["failed_records"] == 2
    assert r3_res["status"] == "FAIL"
