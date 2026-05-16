# Reporting and Results

Guide to viewing, analyzing, and exporting Data Quality check results.

## Table of Contents

- [Understanding Results](#understanding-results)
- [Querying Results](#querying-results)
- [Generating Reports](#generating-reports)
- [Result Analysis](#result-analysis)
- [Visualization](#visualization)

---

## Understanding Results

### Result Structure

Every check execution produces a result record:

```python
{
    "execution_id": "550e8400-e29b-41d4-a716-446655440000",  # UUID for this run
    "rule_id": "chk_user_email",                             # Rule identifier
    "execution_time": datetime(2024, 1, 15, 10, 30, 45),    # When check ran
    "total_records": 50000,                                 # Records evaluated
    "failed_records": 23,                                   # Records failed
    "status": "FAIL",                                       # PASS/FAIL/ERROR
    "error_message": None                                   # Exception details if ERROR
}
```

### Result Storage

Results are stored in the `dq_results` table with the schema:

```
execution_id     STRING
rule_id          STRING
execution_time   TIMESTAMP
total_records    LONG
failed_records   LONG
status           STRING (PASS, FAIL, ERROR)
error_message    STRING (nullable)
```

---

## Querying Results

### Import Reporting Functions

```python
from pyspark_dq.core.reporting import (
    get_last_executed_check_results,
    display_last_executed_check,
    get_checks_on_date,
    display_checks_on_date,
    get_checks_on_date_for_table,
    display_checks_on_date_for_table
)
```

### Get Last Executed Check

Return results from the most recent execution:

```python
from pyspark_dq.core.reporting import get_last_executed_check_results

latest_results = get_last_executed_check_results(
    spark=spark,
    results_table_path="my_db.dq_results"
)

latest_results.show(truncate=False)
```

**Output:**
```
+---------+-----+-----+-----+-----+
|rule_id  |total|failed|status|time |
+---------+-----+-----+-----+-----+
|chk_1    |1000 |5    |PASS |2024...|
|chk_2    |1000 |150  |FAIL |2024...|
+---------+-----+-----+-----+-----+
```

### Display Last Executed Check

Print formatted output to console:

```python
display_last_executed_check(
    spark=spark,
    results_table_path="my_db.dq_results"
)
```

### Get Checks by Date

Query results from a specific date:

```python
from pyspark_dq.core.reporting import get_checks_on_date

date_results = get_checks_on_date(
    spark=spark,
    date_str="2024-01-15",
    results_table_path="my_db.dq_results"
)

date_results.show()
```

### Get Checks by Date and Table

Query results for a specific table on a specific date:

```python
from pyspark_dq.core.reporting import get_checks_on_date_for_table

table_results = get_checks_on_date_for_table(
    spark=spark,
    date_str="2024-01-15",
    table_name="customers",
    results_table_path="my_db.dq_results",
    rules_table_path="my_db.dq_rules"
)

table_results.select(
    "rule_id", "column_name", "check_type", 
    "total_records", "failed_records", "status"
).show(truncate=False)
```

---

## Generating Reports

### HTML Report Generation

#### Option 1: Export to HTML String

```python
from pyspark_dq.core.reporting import export_results_to_html

# Get results dataframe
results_df = spark.read.table("my_db.dq_results").limit(100)

# Generate HTML
html_string = export_results_to_html(results_df)

# Use HTML string (send via email, embed in Airflow, etc.)
print(html_string)
```

#### Option 2: Save HTML Report to File

```python
from pyspark_dq.core.reporting import save_html_report

# Get results dataframe
results_df = spark.read.table("my_db.dq_results").limit(100)

# Save to file
save_html_report(
    results_df=results_df,
    file_path="/path/to/reports/dq_report_2024_01_15.html"
)

print("Report saved to /path/to/reports/dq_report_2024_01_15.html")
```

### Custom HTML Report

Create more sophisticated reports by customizing the HTML template:

```python
from pyspark_dq.core.reporting import export_results_to_html
import datetime

def generate_custom_html_report(spark, results_table_path, title="DQ Report"):
    """Generate a custom HTML report with styling and summary."""
    
    results_df = spark.read.table(results_table_path)
    
    # Collect data
    records = results_df.collect()
    passed = sum(1 for r in records if r.status == "PASS")
    failed = sum(1 for r in records if r.status == "FAIL")
    errors = sum(1 for r in records if r.status == "ERROR")
    total_checks = len(records)
    total_failed_records = sum(r.failed_records for r in records)
    total_records = sum(r.total_records for r in records)
    
    # Generate HTML
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{title}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
            .header {{ background-color: #2c3e50; color: white; padding: 20px; border-radius: 5px; }}
            .summary {{ display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 15px; margin: 20px 0; }}
            .summary-card {{ background: white; padding: 15px; border-radius: 5px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .summary-card h3 {{ margin: 0; color: #7f8c8d; font-size: 12px; text-transform: uppercase; }}
            .summary-card .value {{ font-size: 28px; font-weight: bold; margin-top: 10px; }}
            .summary-card.pass .value {{ color: #27ae60; }}
            .summary-card.fail .value {{ color: #e74c3c; }}
            .summary-card.error .value {{ color: #f39c12; }}
            .summary-card.total .value {{ color: #3498db; }}
            table {{ width: 100%; border-collapse: collapse; background: white; margin-top: 20px; }}
            th {{ background-color: #34495e; color: white; padding: 12px; text-align: left; }}
            td {{ padding: 10px; border-bottom: 1px solid #ecf0f1; }}
            tr:hover {{ background-color: #f8f9fa; }}
            .pass {{ color: #27ae60; font-weight: bold; }}
            .fail {{ color: #e74c3c; font-weight: bold; }}
            .error {{ color: #f39c12; font-weight: bold; }}
            .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #ecf0f1; color: #7f8c8d; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>{title}</h1>
            <p>Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="summary">
            <div class="summary-card pass">
                <h3>Passed</h3>
                <div class="value">{passed}</div>
            </div>
            <div class="summary-card fail">
                <h3>Failed</h3>
                <div class="value">{failed}</div>
            </div>
            <div class="summary-card error">
                <h3>Errors</h3>
                <div class="value">{errors}</div>
            </div>
            <div class="summary-card total">
                <h3>Total Checks</h3>
                <div class="value">{total_checks}</div>
            </div>
        </div>
        
        <h2>Details</h2>
        <table>
            <tr>
                <th>Rule ID</th>
                <th>Execution Time</th>
                <th>Total Records</th>
                <th>Failed Records</th>
                <th>Status</th>
                <th>Error Message</th>
            </tr>
    """
    
    for record in records:
        status_class = record.status.lower()
        error_msg = record.error_message if record.error_message else ""
        html += f"""
            <tr>
                <td>{record.rule_id}</td>
                <td>{record.execution_time}</td>
                <td>{record.total_records:,}</td>
                <td>{record.failed_records:,}</td>
                <td><span class="{status_class}">{record.status}</span></td>
                <td>{error_msg}</td>
            </tr>
        """
    
    html += """
        </table>
        <div class="footer">
            <p>This report was automatically generated by the PySpark Data Quality Framework.</p>
        </div>
    </body>
    </html>
    """
    
    return html

# Generate and save
html = generate_custom_html_report(spark, "my_db.dq_results", "Data Quality Report - 2024-01-15")
with open("dq_custom_report.html", "w") as f:
    f.write(html)
```

---

## Result Analysis

### Summary Statistics

Calculate key metrics from results:

```python
import pyspark.sql.functions as F

def get_dq_summary(spark, results_table_path):
    """Get summary statistics for all DQ checks."""
    
    results_df = spark.read.table(results_table_path)
    
    summary = results_df.agg(
        F.count("rule_id").alias("total_checks"),
        F.sum(F.when(F.col("status") == "PASS", 1).otherwise(0)).alias("passed_checks"),
        F.sum(F.when(F.col("status") == "FAIL", 1).otherwise(0)).alias("failed_checks"),
        F.sum(F.when(F.col("status") == "ERROR", 1).otherwise(0)).alias("error_checks"),
        F.sum("total_records").alias("total_records_evaluated"),
        F.sum("failed_records").alias("total_failed_records"),
        F.avg("failed_records").alias("avg_failed_per_check")
    ).collect()
    
    row = summary[0]
    print(f"""
    📊 DQ SUMMARY
    =============
    Total Checks:          {row.total_checks}
    Passed:                {row.passed_checks}
    Failed:                {row.failed_checks}
    Errors:                {row.error_checks}
    
    Total Records Evaluated: {row.total_records_evaluated:,}
    Total Failed Records:    {row.total_failed_records:,}
    Avg Failed per Check:    {row.avg_failed_per_check:.2f}
    """)
    
    return row

get_dq_summary(spark, "my_db.dq_results")
```

### Failure Analysis

Identify rules that consistently fail:

```python
import pyspark.sql.functions as F

def get_failing_rules(spark, results_table_path, min_failures=3):
    """Identify rules that fail repeatedly."""
    
    results_df = spark.read.table(results_table_path)
    
    failing = results_df.filter(F.col("status") == "FAIL").groupBy("rule_id").agg(
        F.count("*").alias("failure_count"),
        F.avg("failed_records").alias("avg_failed"),
        F.max("execution_time").alias("last_failure")
    ).filter(F.col("failure_count") >= min_failures).orderBy(F.desc("failure_count"))
    
    print("\n⚠️ REPEATEDLY FAILING RULES")
    print("=" * 80)
    failing.show(truncate=False)
    
    return failing

get_failing_rules(spark, "my_db.dq_results")
```

### Time Series Analysis

Track DQ trends over time:

```python
from pyspark.sql.functions import to_date, col, count, sum as spark_sum

def get_dq_trends(spark, results_table_path, days=30):
    """Analyze DQ check results over time."""
    
    results_df = spark.read.table(results_table_path)
    
    trends = results_df.filter(
        col("execution_time") >= F.date_sub(F.current_date(), days)
    ).withColumn("date", to_date("execution_time")).groupBy("date").agg(
        count("*").alias("checks_run"),
        spark_sum(F.when(col("status") == "PASS", 1).otherwise(0)).alias("passed"),
        spark_sum(F.when(col("status") == "FAIL", 1).otherwise(0)).alias("failed"),
        spark_sum("failed_records").alias("total_failed_records")
    ).orderBy("date")
    
    print(f"\n📈 DQ TRENDS (Last {days} days)")
    print("=" * 80)
    trends.show(truncate=False)
    
    return trends

get_dq_trends(spark, "my_db.dq_results", days=30)
```

---

## Visualization

### Export to CSV for External Visualization

```python
import pyspark.sql.functions as F

def export_results_for_viz(spark, results_table_path, output_path="/tmp/dq_results.csv"):
    """Export results to CSV for use in BI tools like Tableau, PowerBI."""
    
    results_df = spark.read.table(results_table_path)
    
    viz_df = results_df.select(
        "execution_id",
        "rule_id",
        F.to_timestamp("execution_time").alias("execution_time"),
        "total_records",
        "failed_records",
        F.round(
            F.col("failed_records") / F.col("total_records") * 100, 2
        ).alias("error_percentage"),
        "status"
    )
    
    viz_df.coalesce(1).write.mode("overwrite").csv(
        output_path,
        header=True
    )
    
    print(f"✅ Results exported to {output_path}")

export_results_for_viz(spark, "my_db.dq_results")
```

### Integration with Databricks Visualization

```python
# In Databricks notebooks, visualize directly:

results_df = spark.read.table("my_db.dq_results")

# Bar chart: Failed records by rule
display(
    results_df.filter(F.col("status") == "FAIL")
              .select("rule_id", "failed_records")
              .orderBy(F.desc("failed_records"))
              .limit(10)
)

# Time series: Checks over time
display(
    results_df.withColumn("date", F.to_date("execution_time"))
              .groupBy("date")
              .agg(F.count("*").alias("checks_run"))
              .orderBy("date")
)
```

---

## Email Reporting

Send reports via email (example with standard Python email):

```python
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pyspark_dq.core.reporting import export_results_to_html

def send_dq_report_email(spark, results_table_path, recipients, smtp_config):
    """Send DQ report via email."""
    
    # Generate report
    results_df = spark.read.table(results_table_path)
    html_body = export_results_to_html(results_df)
    
    # Create email
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Daily Data Quality Report"
    msg["From"] = smtp_config["sender"]
    msg["To"] = ", ".join(recipients)
    
    part = MIMEText(html_body, "html")
    msg.attach(part)
    
    # Send
    with smtplib.SMTP(smtp_config["server"], smtp_config["port"]) as server:
        server.starttls()
        server.login(smtp_config["username"], smtp_config["password"])
        server.sendmail(msg["From"], recipients, msg.as_string())
    
    print(f"✅ Report sent to {recipients}")

# Usage
send_dq_report_email(
    spark,
    "my_db.dq_results",
    recipients=["data-team@company.com"],
    smtp_config={
        "server": "smtp.gmail.com",
        "port": 587,
        "sender": "dq-reports@company.com",
        "username": "your_email",
        "password": "your_password"
    }
)
```

---

## Archiving Results

Archive old results while keeping recent data accessible:

```python
import pyspark.sql.functions as F

def archive_old_results(spark, results_table_path, archive_path, days_to_keep=90):
    """Move old results to archive table."""
    
    results_df = spark.read.table(results_table_path)
    
    # Find old records
    cutoff_date = F.date_sub(F.current_date(), days_to_keep)
    old_results = results_df.filter(F.col("execution_time") < cutoff_date)
    
    # Archive
    old_results.write.mode("append").partitionBy("execution_date").saveAsTable(
        archive_path,
        format="delta"
    )
    
    # Delete from active table
    spark.sql(f"""
        DELETE FROM {results_table_path}
        WHERE execution_time < '{cutoff_date}'
    """)
    
    print(f"✅ Archived {old_results.count()} old result records")

archive_old_results(spark, "my_db.dq_results", "my_db.dq_results_archive")
```

---

## Next Steps

- Learn more about [Check Types](./Check-Types-Reference)
- Explore [Advanced Usage](./Advanced-Usage) patterns
- Review [API Reference](./API-Reference) for detailed methods
