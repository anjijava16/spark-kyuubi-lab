"""
02_wordcount_s3.py <input> <output>

Reads a text file from RustFS, counts words, writes CSV back to RustFS.
Proves the s3a connector works for both reads (driver + executors) and
writes (executors write parts, driver commits).

Example:
  input  = s3a://data/input/sample.txt
  output = s3a://data/output/wordcount_py
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if len(sys.argv) != 3:
    print("usage: 02_wordcount_s3.py <input> <output>", file=sys.stderr)
    sys.exit(2)

src, dst = sys.argv[1], sys.argv[2]
spark = SparkSession.builder.appName("02-wordcount-s3").getOrCreate()

lines = spark.read.text(src)
words = (lines
         .select(F.explode(F.split(F.lower(F.col("value")), r"[^a-z0-9']+")).alias("word"))
         .where(F.length("word") > 2))
counts = words.groupBy("word").count().orderBy(F.desc("count"), "word")

counts.coalesce(1).write.mode("overwrite").option("header", True).csv(dst)

print(f"Read {lines.count()} lines from {src}")
print(f"Wrote {counts.count()} distinct words to {dst}")
counts.show(10, truncate=False)

# read back what we wrote, so a silent write failure can't pass
back = spark.read.option("header", True).csv(dst).count()
assert back == counts.count(), f"read back {back} rows, expected {counts.count()}"
print("WORDCOUNT OK")
spark.stop()
