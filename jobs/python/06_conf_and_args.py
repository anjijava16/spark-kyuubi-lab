"""
06_conf_and_args.py --run-date YYYY-MM-DD --env NAME

Shows the two ways to pass settings into a job and checks both arrive:
  * app arguments (the "args" list in the batch request)
  * Spark conf    (the "conf" map in the batch request)
Custom keys must start with "spark." or Spark drops them.
"""
import argparse

from pyspark.sql import SparkSession

p = argparse.ArgumentParser()
p.add_argument("--run-date", required=True)
p.add_argument("--env", default="dev")
args = p.parse_args()

spark = SparkSession.builder.appName("06-conf-and-args").getOrCreate()
conf = spark.sparkContext.getConf()

team = conf.get("spark.app.custom.team", "<not set>")
exec_mem = conf.get("spark.executor.memory", "<not set>")
print(f"run-date={args.run_date} env={args.env}")
print(f"spark.app.custom.team={team} spark.executor.memory={exec_mem}")
print(f"spark.driver.host={conf.get('spark.driver.host', '<not set>')}")

assert team == "data-eng", "custom conf did not reach the driver"
print("CONF AND ARGS OK")
spark.stop()
