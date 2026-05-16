import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder \
        .appName("pytest-pyspark-dq") \
        .master("local[2]") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()
