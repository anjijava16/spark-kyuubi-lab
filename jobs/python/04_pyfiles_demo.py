"""
04_pyfiles_demo.py <text_file>

Imports a helper module that is NOT inside this script. It arrives via
--py-files s3a://jobs/python/libs/text_utils.zip. The import inside the
lambda runs on the executors, so this also proves the zip got shipped to
the worker, not just to the driver.

Submit with:
  kyuubi_submit.py submit s3a://jobs/python/04_pyfiles_demo.py --remote \
      --py-files s3a://jobs/python/libs/text_utils.zip -- s3a://data/input/sample.txt
"""
import sys

from pyspark.sql import SparkSession

import text_utils  # driver-side import; fails fast if the zip is missing

src = sys.argv[1] if len(sys.argv) > 1 else "s3a://data/input/sample.txt"
spark = SparkSession.builder.appName("04-pyfiles-demo").getOrCreate()
sc = spark.sparkContext

print(f"text_utils version on driver: {text_utils.VERSION}")


def clean(line):
    import text_utils as tu  # executor-side import
    return tu.normalize_words(line)


words = sc.textFile(src).flatMap(clean)
top = words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b) \
           .takeOrdered(5, key=lambda kv: -kv[1])
print("Top words (stop words removed by text_utils):", top)

versions = sc.parallelize(range(4), 4).map(lambda _: __import__("text_utils").VERSION).distinct().collect()
print(f"text_utils version on executors: {versions}")
assert versions == [text_utils.VERSION]
print("PYFILES OK")
spark.stop()
