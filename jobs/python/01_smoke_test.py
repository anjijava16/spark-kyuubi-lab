"""
01_smoke_test.py - the "is anything working at all" job.

Checks: driver starts, executors on the worker run Python tasks, and the
result comes back correct. No storage, no Hive. If this fails, nothing
else will pass.

Note: no .master() here. The master comes from Kyuubi / spark-defaults.
Hard-coding it in the script is the most common reason a job that ran
on a laptop breaks on a cluster.
"""
import platform
import socket

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("01-smoke-test").getOrCreate()
sc = spark.sparkContext

print(f"Spark version : {spark.version}")
print(f"Python        : {platform.python_version()}")
print(f"Master        : {sc.master}")
print(f"Driver host   : {socket.gethostname()}")

n = 1_000_000
total = spark.range(n).selectExpr("sum(id) AS s").first()["s"]
expected = n * (n - 1) // 2
print(f"sum(0..{n - 1}) = {total} (expected {expected})")
assert total == expected, "wrong sum"

# Run real Python on the executors and report where it ran
hosts = (sc.parallelize(range(16), 8)
           .map(lambda _: socket.gethostname())
           .distinct()
           .collect())
print(f"Python tasks ran on: {hosts}")

print("SMOKE TEST OK")
spark.stop()
