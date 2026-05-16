from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

def get_last_executed_check_results(spark: SparkSession, results_table_path: str) -> DataFrame:
    """Returns the results of the last executed check based on execution_time."""
    df = spark.read.table(results_table_path)
    # Find the latest execution_time
    latest_time_row = df.select(F.max("execution_time").alias("max_time")).first()
    if not latest_time_row or latest_time_row["max_time"] is None:
        return spark.createDataFrame([], df.schema)
        
    latest_time = latest_time_row["max_time"]
    return df.filter(F.col("execution_time") == latest_time)

def display_last_executed_check(spark: SparkSession, results_table_path: str):
    """Displays the results of the last executed check."""
    df = get_last_executed_check_results(spark, results_table_path)
    print("--- Results for Last Executed Check ---")
    df.show(truncate=False)

def get_checks_on_date(spark: SparkSession, date_str: str, results_table_path: str) -> DataFrame:
    """Returns results for a specific date (YYYY-MM-DD)."""
    df = spark.read.table(results_table_path)
    return df.filter(F.to_date(F.col("execution_time")) == F.lit(date_str))

def display_checks_on_date(spark: SparkSession, date_str: str, results_table_path: str):
    """Displays checks executed on a specific date (YYYY-MM-DD)."""
    df = get_checks_on_date(spark, date_str, results_table_path)
    print(f"--- Results for Date: {date_str} ---")
    df.show(truncate=False)

def get_checks_on_date_for_table(
    spark: SparkSession, 
    date_str: str, 
    table_name: str, 
    results_table_path: str, 
    rules_table_path: str
) -> DataFrame:
    """Returns results for a specific date and table by joining results and rules."""
    results_df = spark.read.table(results_table_path)
    rules_df = spark.read.table(rules_table_path)
    
    joined_df = results_df.join(rules_df, on="rule_id", how="inner")
    
    filtered_df = joined_df.filter(
        (F.to_date(F.col("execution_time")) == F.lit(date_str)) & 
        (F.col("table_name") == F.lit(table_name))
    )
    return filtered_df

def display_checks_on_date_for_table(
    spark: SparkSession, 
    date_str: str, 
    table_name: str, 
    results_table_path: str, 
    rules_table_path: str
):
    """Displays checks for a specific date and table."""
    df = get_checks_on_date_for_table(spark, date_str, table_name, results_table_path, rules_table_path)
    # Select columns to display clearly
    display_df = df.select(
        "execution_time", "rule_id", "table_name", "column_name", 
        "check_type", "total_records", "failed_records", "status"
    )
    print(f"--- Results for Date: {date_str} and Table: {table_name} ---")
    display_df.show(truncate=False)

def export_results_to_html(results_df: DataFrame) -> str:
    """
    Exports a PySpark DataFrame to an HTML string.
    Collects the dataframe to the driver, so use with appropriately sized result sets.
    """
    records = results_df.collect()
    columns = results_df.columns
    
    html = [
        "<table border='1' style='border-collapse: collapse; width: 100%; font-family: Arial, sans-serif;'>"
    ]
    
    # Header
    html.append("  <tr style='background-color: #f2f2f2;'>")
    for col in columns:
        html.append(f"    <th style='padding: 8px; text-align: left;'>{col}</th>")
    html.append("  </tr>")
    
    # Rows
    for row in records:
        html.append("  <tr>")
        for col in columns:
            val = row[col]
            # Convert values to strings
            html.append(f"    <td style='padding: 8px;'>{str(val) if val is not None else ''}</td>")
        html.append("  </tr>")
        
    html.append("</table>")
    return "\n".join(html)

def save_html_report(results_df: DataFrame, file_path: str):
    """Generates an HTML report and saves it to a file."""
    html_content = export_results_to_html(results_df)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"HTML report successfully saved to {file_path}")
