# PySpark Data Quality Framework

A lightweight, metadata-driven data quality (DQ) utility for PySpark applications. This framework evaluates dataframes against a set of predefined rules using a **"One Rule at a Time"** iterative execution architecture.

## Features

- **Metadata-Driven:** Define your DQ rules as data (in a Lakehouse schema or passing at runtime) rather than hardcoding checks.
- **SQL Filtering:** Support for applying SQL `WHERE` clauses (`filter_condition`) directly within rules before evaluating the check.
- **Batch Evaluation & Mandates:** Determine batch success/failure utilizing the `is_mandatory` rule flag.
- **Extensible Standard Checks:** Supports standard checks (nulls, duplicates, ranges) along with custom SQL evaluation, schema matching, and regex formats (email, phone, pincode).
- **Out-of-the-Box Reporting:** Extract execution histories and save detailed HTML reports.

## Setup & Installation

Install requirements:
```bash
pip install -r requirements.txt
```

### Creating Tables

Initialize your backend Lakehouse schemas and tables (Delta/Parquet) for storing rules and results:

```python
from pyspark.sql import SparkSession
from pyspark_dq.utils.setup import create_dq_tables

spark = SparkSession.builder.appName("DQ-App").getOrCreate()

# Automatically provision `dq_rules` and `dq_results` in your warehouse
create_dq_tables(
    spark=spark, 
    schema_name="my_database",
    format="delta",
    create_schema=False # Set to True if schema needs to be created
)
```

## How It Works

1. **Define Rules**: Save a list of dictionaries to your `dq_rules` table using the framework schema.
2. **Load Rules**: Extract active rules for a specific table.
3. **Execute**: The `DQEngine` evaluates the dataframe against the loaded rules sequentially.
4. **Save Results**: Write the resulting metrics back to your `dq_results` table.

### Rules Schema Definition

| Column | Type | Description |
| :--- | :--- | :--- |
| `rule_id` | String | Unique identifier for the rule. |
| `table_name` | String | Target dataframe name. |
| `column_name` | String | Target column name (Nullable for table-level checks). |
| `filter_condition`| String | **SQL WHERE clause** applied before evaluating the rule. |
| `check_type` | String | Defines the function to run (e.g., `is_not_null`, `value_in_range`). |
| `check_against_value`| String | Target value (JSON, string, int, regex, or SQL expression). |
| `threshold` | Double | Allowed error percentage (e.g. `0.05` for 5%). Defaults to `0.0`. |
| `is_active` | Boolean | Whether to include this rule in the current run. |
| `is_mandatory` | Boolean | If `True`, failing this rule flips the entire batch status to `FAIL`. |
| `severity` | String | (Optional) Error, Warning, Fatal. |

## Supported Check Types & Examples

Here is a comprehensive list of all supported check types, along with JSON rule definitions demonstrating how to configure them.

### Basic Checks
```json
[
  {
    "rule_id": "chk_null",
    "check_type": "is_not_null",
    "column_name": "user_id"
  },
  {
    "rule_id": "chk_unique",
    "check_type": "is_unique",
    "column_name": "email"
  },
  {
    "rule_id": "chk_duplicate_combo",
    "check_type": "duplicates",
    "column_name": "first_name, last_name" 
  },
  {
    "rule_id": "chk_numeric",
    "check_type": "is_numeric",
    "column_name": "amount_str"
  },
  {
    "rule_id": "chk_length",
    "check_type": "length_equal",
    "column_name": "country_code",
    "check_against_value": "2"
  }
]
```

### Range Validation (`value_in_range`)
Validates if the column falls inside a specific range `[min, max]`. Natively supports numbers and dates.
```json
[
  {
    "rule_id": "chk_range_int",
    "check_type": "value_in_range",
    "column_name": "age",
    "check_against_value": "[18, 99]"
  },
  {
    "rule_id": "chk_range_float",
    "check_type": "value_in_range",
    "column_name": "salary",
    "check_against_value": "[30000.50, 150000.00]"
  },
  {
    "rule_id": "chk_range_date",
    "check_type": "value_in_range",
    "column_name": "signup_date",
    "check_against_value": "[\"2023-01-01\", \"2023-12-31\"]"
  }
]
```

### Date Formatting (`date_format`)
Validates if the string can be parsed using the target format.
```json
[
  {
    "rule_id": "chk_date_fmt_1",
    "check_type": "date_format",
    "column_name": "birth_date",
    "check_against_value": "yyyy-MM-dd"
  },
  {
    "rule_id": "chk_date_fmt_2",
    "check_type": "date_format",
    "column_name": "event_timestamp",
    "check_against_value": "yyyy-MM-dd HH:mm:ss"
  },
  {
    "rule_id": "chk_date_fmt_3",
    "check_type": "date_format",
    "column_name": "transaction_month",
    "check_against_value": "yyyy/MM"
  }
]
```

### Custom SQL and Filter Conditions
You can apply `filter_condition` to ANY rule to only evaluate specific rows. `custom_sql` allows writing complex row-level validations.
```json
[
  {
    "rule_id": "chk_filter_1",
    "check_type": "is_not_null",
    "column_name": "bonus_amount",
    "filter_condition": "employee_type = 'FULL_TIME' AND status = 'ACTIVE'"
  },
  {
    "rule_id": "chk_filter_2",
    "check_type": "value_in_list",
    "column_name": "department",
    "check_against_value": "[\"HR\", \"IT\"]",
    "filter_condition": "region = 'US'"
  },
  {
    "rule_id": "chk_custom_sql_1",
    "check_type": "custom_sql",
    "check_against_value": "amount > 0 AND amount <= credit_limit"
  },
  {
    "rule_id": "chk_custom_sql_2",
    "check_type": "custom_sql",
    "check_against_value": "end_date >= start_date"
  }
]
```

### Comparisons and Valid Lists
```json
[
  {
    "rule_id": "chk_eq",
    "check_type": "is_equal_to",
    "column_name": "status",
    "check_against_value": "ACTIVE"
  },
  {
    "rule_id": "chk_gt",
    "check_type": "is_greater_than",
    "column_name": "age",
    "check_against_value": "18"
  },
  {
    "rule_id": "chk_lt",
    "check_type": "is_less_than",
    "column_name": "discount_rate",
    "check_against_value": "0.5"
  },
  {
    "rule_id": "chk_in_list",
    "check_type": "value_in_list",
    "column_name": "priority",
    "check_against_value": "[\"HIGH\", \"MEDIUM\", \"LOW\"]"
  }
]
```

### Regex and Schema Match
```json
[
  {
    "rule_id": "chk_email",
    "check_type": "valid_email",
    "column_name": "email_address"
  },
  {
    "rule_id": "chk_phone_custom",
    "check_type": "valid_phone",
    "column_name": "phone_number",
    "check_against_value": "^\\+1\\d{10}$" 
  },
  {
    "rule_id": "chk_pincode_custom",
    "check_type": "valid_pincode",
    "column_name": "zip_code",
    "check_against_value": "^\\d{5}(-\\d{4})?$" 
  },
  {
    "rule_id": "chk_schema",
    "check_type": "schema_match",
    "check_against_value": "{\"id\": \"int\", \"name\": \"string\", \"price\": \"double\"}"
  }
]
```

## Execution Examples

### 1. Ad-Hoc Execution (Without Saving Rules)
You can define rules on the fly and execute them directly against a specific dataframe, utilizing specific column combinations.

```python
from pyspark_dq.core.engine import DQEngine

# Define on-the-fly rules
adhoc_rules = [
    {
        "rule_id": "adhoc_duplicates",
        "check_type": "duplicates",
        "column_name": "first_name, last_name, date_of_birth" # Column combination
    },
    {
        "rule_id": "adhoc_nulls",
        "check_type": "is_not_null",
        "column_name": "email",
        "is_mandatory": True
    }
]

engine = DQEngine(spark)

# Execute against the dataframe
execution_output = engine.execute_rules(df=my_dataframe, rules=adhoc_rules)

if execution_output["batch_status"] == "FAIL":
    print("Mandatory ad-hoc rules failed!")
```

### 2. Saving Rules and Executing Against a Specific Table
A more traditional pipeline where rules are saved and managed centrally.

```python
from pyspark_dq.core.metadata import save_rules, load_active_rules_from_list
from pyspark_dq.core.engine import DQEngine

table_rules = [
    {
        "rule_id": "users_rule_1",
        "table_name": "users",
        "column_name": "age",
        "check_type": "value_in_range",
        "check_against_value": "[18, 120]",
        "is_active": True
    }
]

# Save rules to the backend
save_rules(spark, rules_list=table_rules, rules_table_path="my_database.dq_rules")

# Load active rules specific to the 'users' table
active_users_rules = load_active_rules_from_list(table_rules, "users")

engine = DQEngine(spark)
execution_output = engine.execute_rules(df=users_df, rules=active_users_rules)
```

### 3. Executing Against Multiple Tables
If you have a pipeline processing multiple tables, you can iteratively filter your master rules list.

```python
# Assuming 'master_rules' contains rules for various tables
tables_to_process = {
    "users": users_df,
    "transactions": transactions_df
}

engine = DQEngine(spark)
all_execution_results = []

for table_name, df in tables_to_process.items():
    # Filter rules for the current table
    current_table_rules = load_active_rules_from_list(master_rules, table_name)
    
    # Execute if rules exist
    if current_table_rules:
        output = engine.execute_rules(df=df, rules=current_table_rules)
        all_execution_results.extend(output["results"])
        
        if output["batch_status"] == "FAIL":
            print(f"Warning: Data Quality failed for table {table_name}")
```

### 4. Saving Results
Regardless of how you executed the rules, you can save the consolidated results back to the database.

```python
from pyspark_dq.core.results import save_results

# 'all_execution_results' is the list of result dictionaries from the engine
results_df = save_results(
    spark=spark, 
    results_list=all_execution_results, 
    results_table_path="my_database.dq_results"
)
```

### 5. Reporting (Exporting and Returning HTML)
The framework provides utilities to generate visual reports from the execution results dataframe.

```python
from pyspark_dq.core.reporting import save_html_report, export_results_to_html, display_last_executed_check

# Option A: Save the report directly to an HTML file
save_html_report(results_df, "dq_report_latest.html")

# Option B: Return the HTML as a string (useful for sending via Email or embedding in Airflow/Databricks)
html_string = export_results_to_html(results_df)
# print(html_string) # Send this string via your notification service

# You can also quickly display results in the console:
display_last_executed_check(spark, "my_database.dq_results")
```

### For complete documentation refer  [wiki pages](https://github.com/saurabhr2/pyspark-dq/wiki)

## Owner & Contributor
#Owner: [Saurabh Rai](saurabhr2@gmail.com)

#Contributors:
