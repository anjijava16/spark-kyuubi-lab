package com.example;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * JavaWordCount &lt;input&gt; &lt;output&gt;
 *
 * Same job as 02_wordcount_s3.py, in Java, so you can compare the two
 * submission paths (resource = .py vs resource = .jar + className).
 */
public final class JavaWordCount {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("usage: JavaWordCount <input> <output>");
            System.exit(2);
        }
        String input = args[0];
        String output = args[1];

        SparkSession spark = SparkSession.builder()
                .appName("java-wordcount")
                .getOrCreate();

        Dataset<Row> lines = spark.read().text(input);
        Dataset<Row> counts = lines
                .select(explode(split(lower(col("value")), "[^a-z0-9']+")).as("word"))
                .where(length(col("word")).gt(2))
                .groupBy("word").count()
                .orderBy(desc("count"), col("word"));

        counts.coalesce(1).write().mode("overwrite").option("header", "true").csv(output);

        long written = spark.read().option("header", "true").csv(output).count();
        System.out.println("Spark " + spark.version() + ", wrote " + written + " words to " + output);
        counts.show(10, false);

        if (written != counts.count()) {
            throw new IllegalStateException("read back " + written + " rows, expected " + counts.count());
        }
        System.out.println("JAVA WORDCOUNT OK");
        spark.stop();
    }
}
