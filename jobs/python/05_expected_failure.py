"""
05_expected_failure.py - fails on purpose.

A test harness that only ever sees green is not testing much. This job
starts Spark, does a little work, then raises. Kyuubi should mark the
batch ERROR, and the driver log should contain the exception text.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("05-expected-failure").getOrCreate()
print("rows:", spark.range(100).count())
raise RuntimeError("Intentional failure from 05_expected_failure.py")
