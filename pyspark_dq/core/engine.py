import uuid
import datetime
from typing import List, Dict
from pyspark.sql import DataFrame
from pyspark_dq.checks.standard_checks import CHECK_REGISTRY

class DQEngine:
    def __init__(self, spark_session):
        self.spark = spark_session

    def execute_rules(self, df: DataFrame, rules: List[Dict], execution_id: str = None) -> dict:
        """
        Iterates over the provided rules ONE AT A TIME and applies them to the dataframe.
        Returns a dictionary with 'batch_status' (PASS/FAIL) and the 'results' list.
        """
        if not execution_id:
            execution_id = str(uuid.uuid4())
            
        results = []
        batch_status = "PASS"
        
        for rule in rules:
            rule_id = rule.get("rule_id", "unknown")
            check_type = rule.get("check_type")
            threshold = rule.get("threshold", 0.0)
            is_mandatory = rule.get("is_mandatory", False)
            
            result_record = {
                "execution_id": execution_id,
                "rule_id": rule_id,
                "execution_time": datetime.datetime.now(),
                "total_records": 0,
                "failed_records": 0,
                "status": "ERROR",
                "error_message": None
            }
            
            try:
                check_func = CHECK_REGISTRY.get(check_type)
                if not check_func:
                    raise ValueError(f"Unsupported check_type: {check_type}")
                
                # Execute the specific check function
                check_result = check_func(df, rule)
                
                total = check_result.get("total_records", 0)
                failed = check_result.get("failed_records", 0)
                
                result_record["total_records"] = total
                result_record["failed_records"] = failed
                
                if total == 0:
                    result_record["status"] = "PASS" # No data to fail
                else:
                    error_rate = failed / total
                    if error_rate <= threshold:
                        result_record["status"] = "PASS"
                    else:
                        result_record["status"] = "FAIL"
                        if is_mandatory:
                            batch_status = "FAIL"
                        
            except Exception as e:
                result_record["status"] = "ERROR"
                result_record["error_message"] = str(e)
                if is_mandatory:
                    batch_status = "FAIL"
                
            results.append(result_record)
            
        return {
            "batch_status": batch_status,
            "results": results
        }
