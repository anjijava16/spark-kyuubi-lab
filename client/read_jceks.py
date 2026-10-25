"""
my_job.py - Spark job that reads DB passwords from a .jceks file shipped with --files.

spark-submit (YARN cluster mode):
    spark-submit --master yarn --deploy-mode cluster \
        --files /path/to/db.jceks \
        my_job.py

Local test (just checks the secrets can be read, no DB call):
    spark-submit --files ./db.jceks my_job.py --check
"""
import os
import sys

from pyspark import SparkFiles
from pyspark.sql import SparkSession

JCEKS_NAME = "db.jceks"          # file name as passed in --files


def find_jceks(name=JCEKS_NAME):
    """Locate the file shipped with --files (works in client and cluster mode)."""
    candidates = [
        os.path.abspath(name),          # YARN cluster mode: container working dir
    ]
    try:
        candidates.append(SparkFiles.get(name))   # client / local mode
    except Exception:
        pass
    for path in candidates:
        if os.path.isfile(path):
            return path
    raise FileNotFoundError(f"{name} not found, looked in: {candidates}. "
                            f"Did you pass --files {name}?")


def get_secret(spark, alias, jceks_file=None):
    """Read one alias from the .jceks. No keystore password needed (Hadoop default)."""
    jceks_file = jceks_file or find_jceks()
    jvm = spark._jvm

    hconf = spark._jsc.hadoopConfiguration()
    hconf.set("hadoop.security.credential.provider.path", "jceks://file" + jceks_file)
    chars = hconf.getPassword(alias.lower())
    if chars is not None:
        return str(jvm.java.lang.String(chars))

    # getPassword() also falls back to a plain config key with the same name,
    # so None here really means the alias isn't in the file.
    raise KeyError(f"alias '{alias}' not found in {jceks_file}")


def main():
    spark = SparkSession.builder.appName("td-sql-job").getOrCreate()

    td_password = get_secret(spark, "teradata.password")
    sql_password = get_secret(spark, "sqlserver.password")

    if "--check" in sys.argv:
        # never print the real values
        print(f"teradata.password  -> read OK ({len(td_password)} chars)")
        print(f"sqlserver.password -> read OK ({len(sql_password)} chars)")
        spark.stop()
        return

    # ---- Teradata ----
    td_df = (spark.read.format("jdbc")
             .option("url", "jdbc:teradata://td-host.example.com/DATABASE=mydb,LOGMECH=TD2")
             .option("driver", "com.teradata.jdbc.TeraDriver")
             .option("user", "td_user")
             .option("password", td_password)
             .option("dbtable", "mydb.my_table")
             .load())
    td_df.show(5)

    # ---- SQL Server ----
    sql_df = (spark.read.format("jdbc")
              .option("url", "jdbc:sqlserver://sql-host.example.com:1433;databaseName=mydb;encrypt=true;trustServerCertificate=true")
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
              .option("user", "sql_user")
              .option("password", sql_password)
              .option("dbtable", "dbo.my_table")
              .load())
    sql_df.show(5)

    spark.stop()


if __name__ == "__main__":
    main()
