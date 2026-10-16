"""
10_format_conversions.py [input_csv] [output_root]

Read one format, write another, five times in a row, then prove nothing
was lost on the way:

    CSV -> Parquet -> ORC -> JSON -> Avro -> CSV
                 \\-> Parquet partitioned by region

Each hop reads the files the previous hop wrote (not the DataFrame in
memory), so every reader and writer is really exercised. At the end the
last CSV must equal the first CSV row for row, and the job prints how big
the same data is in each format.

Defaults: s3a://data/input/orders.csv -> s3a://data/output/formats/
Needs spark-avro on the classpath (baked into the lab's Spark image).
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

src = sys.argv[1] if len(sys.argv) > 1 else "s3a://data/input/orders.csv"
root = (sys.argv[2] if len(sys.argv) > 2 else "s3a://data/output/formats").rstrip("/")

spark = SparkSession.builder.appName("10-format-conversions").getOrCreate()

# One explicit schema for the text formats (CSV, JSON), which carry no
# types of their own. Parquet, ORC and Avro store the schema in the file.
SCHEMA = StructType([
    StructField("order_id", IntegerType()),
    StructField("order_date", DateType()),
    StructField("region", StringType()),
    StructField("product", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("customer_id", IntegerType()),
])
COLS = [f.name for f in SCHEMA.fields]


def size_of(path):
    """Total bytes under a path, via the Hadoop FileSystem API (works for s3a)."""
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.getContentSummary(p).getLength()


original = spark.read.option("header", True).schema(SCHEMA).csv(src)
n = original.count()
print(f"source: {src}  rows={n}")

# Each hop: (name, writer, reader). Readers get the schema only where the
# format has none of its own.
hops = [
    ("parquet", lambda df, p: df.write.mode("overwrite").option("compression", "snappy").parquet(p),
                lambda p: spark.read.parquet(p)),
    ("orc",     lambda df, p: df.write.mode("overwrite").option("compression", "zlib").orc(p),
                lambda p: spark.read.orc(p)),
    ("json",    lambda df, p: df.write.mode("overwrite").json(p),
                lambda p: spark.read.schema(SCHEMA).json(p)),
    ("avro",    lambda df, p: df.write.mode("overwrite").format("avro").save(p),
                lambda p: spark.read.format("avro").load(p)),
    ("csv_roundtrip", lambda df, p: df.coalesce(1).write.mode("overwrite").option("header", True).csv(p),
                      lambda p: spark.read.option("header", True).schema(SCHEMA).csv(p)),
]

current = original
sizes = [("csv (source)", size_of(src))]
for name, write, read in hops:
    path = f"{root}/{name}"
    write(current.select(*COLS), path)
    current = read(path).select(*COLS)        # next hop reads what was just written
    count = current.count()
    sizes.append((name, size_of(path)))
    print(f"  -> {name:14} rows={count:4}  {path}")
    assert count == n, f"{name}: expected {n} rows, got {count}"

# Partitioned Parquet: one directory per region, the layout most lakes use
part_path = f"{root}/parquet_by_region"
original.write.mode("overwrite").partitionBy("region").parquet(part_path)
north = spark.read.parquet(part_path).where("region = 'North'")   # prunes to one folder
print(f"  -> parquet_by_region  North rows={north.count()} (partition pruning)")
sizes.append(("parquet_by_region", size_of(part_path)))

# The point of the exercise: after five conversions, same data
missing = original.exceptAll(current).count()
extra = current.exceptAll(original).count()
print(f"round trip: missing={missing} extra={extra}")
assert missing == 0 and extra == 0, "data changed during format conversions"

print("\nsize of the same data per format:")
for name, b in sizes:
    print(f"  {name:20} {b:>9,} bytes")

current.groupBy("region").agg(F.count("*").alias("orders"),
                              F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("revenue")) \
       .orderBy("region").show()
print("FORMAT CONVERSIONS OK")
spark.stop()
