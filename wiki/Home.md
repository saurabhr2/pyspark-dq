# PySpark Data Quality Framework - Wiki

Welcome to the **PySpark Data Quality (DQ) Framework** wiki! This is a lightweight, metadata-driven data quality utility for PySpark applications.

## Quick Navigation

- **[Getting Started](./Getting-Started)** - Setup instructions and initial configuration
- **[Architecture & Design](./Architecture)** - Framework design and component overview
- **[Usage Guide](./Usage-Guide)** - How to use the framework with examples
- **[Check Types Reference](./Check-Types-Reference)** - Complete list of all supported checks
- **[API Reference](./API-Reference)** - Detailed API documentation
- **[Advanced Usage](./Advanced-Usage)** - Filters, custom SQL, and complex scenarios
- **[Reporting & Results](./Reporting-and-Results)** - How to generate and export reports
- **[FAQ](./FAQ)** - Frequently asked questions

## Key Features

✅ **Metadata-Driven** - Define rules as data, not code  
✅ **One Rule at a Time** - Iterative evaluation with detailed tracking  
✅ **SQL Filtering** - Apply WHERE clauses before evaluating checks  
✅ **Batch Mandates** - Mark critical rules to fail entire batches  
✅ **16+ Check Types** - From nulls to regex patterns to custom SQL  
✅ **HTML Reporting** - Generate visual reports of check results  
✅ **Extensible** - Easy to add custom check functions  

## Framework Overview

The framework operates on these core principles:

1. **Define Rules** - Create rule definitions specifying what to check
2. **Load Rules** - Retrieve active rules from storage or pass inline
3. **Execute** - Run the DQEngine against your dataframe with loaded rules
4. **Report** - Analyze results and generate reports

## Architecture

```
┌─────────────────────────────────────────────────────┐
│           PySpark DataFrame                         │
│  (users, transactions, events, etc.)                │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│         DQEngine (core/engine.py)                   │
│  - Orchestrates rule execution                      │
│  - Tracks batch_status (PASS/FAIL)                  │
│  - Calls CHECK_REGISTRY functions                   │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│    Standard Checks (checks/standard_checks.py)      │
│  - 16+ check functions                              │
│  - Applies filters before evaluation                │
│  - Returns {total_records, failed_records}          │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│         Results DataFrame                           │
│  - execution_id, rule_id, total_records, failed ... │
│  - status: PASS/FAIL/ERROR                          │
└─────────────────────────────────────────────────────┘
```

## Basic Example

```python
from pyspark.sql import SparkSession
from pyspark_dq.core.engine import DQEngine

spark = SparkSession.builder.appName("DQ-App").getOrCreate()

# Create sample dataframe
df = spark.createDataFrame(
    [(1, "alice@example.com"), (2, None)],
    ["id", "email"]
)

# Define rules inline
rules = [
    {
        "rule_id": "email_not_null",
        "check_type": "is_not_null",
        "column_name": "email",
        "is_mandatory": True
    }
]

# Execute rules
engine = DQEngine(spark)
output = engine.execute_rules(df=df, rules=rules)

print(output["batch_status"])  # FAIL (mandatory rule failed)
print(output["results"])       # Detailed check results
```

## Getting Started

Ready to dive in? Start with **[Getting Started](./Getting-Started)** for installation and setup instructions.

---

**Version:** 0.1.0  
**License:** See repository LICENSE  
**Report Issues:** Use GitHub Issues
