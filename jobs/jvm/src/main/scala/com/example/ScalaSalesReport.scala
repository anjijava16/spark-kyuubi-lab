package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * ScalaSalesReport [sourceTable] [targetTable]
 *
 * Reads the Hive table written by 03_hive_etl.py, aggregates it, and writes
 * a second Hive table. Python wrote it, Scala reads it: the metastore is
 * the thing that makes that work, so this is the cross-language check.
 */
object ScalaSalesReport {

  def main(args: Array[String]): Unit = {
    val source = if (args.length > 0) args(0) else "demo.orders"
    val target = if (args.length > 1) args(1) else "demo.sales_summary"

    val spark = SparkSession.builder()
      .appName("scala-sales-report")
      .enableHiveSupport()
      .getOrCreate()

    if (!spark.catalog.tableExists(source)) {
      throw new IllegalStateException(
        s"$source not found in the metastore. Run 03_hive_etl.py first.")
    }

    val summary = spark.table(source)
      .groupBy("region", "product")
      .agg(
        count(lit(1)).as("orders"),
        sum("quantity").as("units"),
        round(sum("amount"), 2).as("revenue"))
      .orderBy(desc("revenue"))

    summary.write.mode("overwrite").format("parquet").saveAsTable(target)

    println(s"Spark ${spark.version}: $source -> $target")
    spark.table(target).show(20, truncate = false)

    val total = spark.table(target).agg(sum("orders")).first().getLong(0)
    val srcRows = spark.table(source).count()
    require(total == srcRows, s"summary covers $total orders, source has $srcRows")
    println("SCALA SALES REPORT OK")
    spark.stop()
  }
}
