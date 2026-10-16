#!/bin/bash
# Downloads optional connector jars that match the lab's Spark version into
# /opt/lakehouse/<name>/ (a shared Docker volume mounted read-only into
# kyuubi and the Spark containers):
#
#   delta    Delta Lake          iceberg  Apache Iceberg
#   hudi     Apache Hudi         kafka    Spark's Kafka connector
#
# Why a volume instead of /opt/spark/jars: Delta, Iceberg and Hudi each want
# to own spark_catalog / session extensions, so loading all of them into
# every job makes them fight. A job opts in to what it needs by putting
# /opt/lakehouse/<name>/* on its driver+executor classpath; the --with flag
# of client/kyuubi_submit.py does that.
#
# Maven does the downloading, so transitive runtime deps come along (Delta's
# delta-storage, Kafka's kafka-clients) and a wrong version fails loudly here.
set -euo pipefail

OUT=/opt/lakehouse
SPARK_MINOR="${SPARK_VERSION%.*}"     # 4.1.3 -> 4.1
SCALA=2.13
# Already in the Spark image; second copies on the classpath cause conflicts.
SPARK_PROVIDED="org.apache.spark,org.scala-lang,org.scala-lang.modules,org.apache.hadoop,org.slf4j,org.apache.logging.log4j,com.fasterxml.jackson.core,com.fasterxml.jackson.module,org.antlr"
# Same idea for the Kafka connector, but we must keep org.apache.spark there
# (spark-sql-kafka and spark-token-provider-kafka ARE what we want).
KAFKA_PROVIDED="org.scala-lang,org.scala-lang.modules,org.slf4j,org.lz4,org.xerial.snappy,com.github.luben,com.google.code.findbugs"

fetch() {
  # fetch <dir-name> <exclude-groups|"bundle"> <group:artifact:version>
  local name=$1 excludes=$2 coord=$3
  local dir="$OUT/$name"
  if [[ -f "$dir/.coordinate" && "$(cat "$dir/.coordinate")" == "$coord" ]]; then
    echo ">> $name: $coord already present, skipping"
    return
  fi
  echo ">> $name: resolving $coord"
  IFS=: read -r g a v <<<"$coord"
  local work; work=$(mktemp -d)
  cat > "$work/pom.xml" <<EOF
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>lab</groupId><artifactId>fetch-$name</artifactId><version>1</version>
  <dependencies>
    <dependency><groupId>$g</groupId><artifactId>$a</artifactId><version>$v</version></dependency>
  </dependencies>
</project>
EOF
  rm -rf "$dir.tmp" && mkdir -p "$dir.tmp"
  local extra=()
  if [[ "$excludes" == "bundle" ]]; then
    extra+=(-DexcludeTransitive=true)          # shaded: everything is in the one jar
  else
    extra+=(-DexcludeGroupIds="$excludes")
  fi
  mvn -B -q -f "$work/pom.xml" dependency:copy-dependencies \
      -DincludeScope=runtime -DoutputDirectory="$dir.tmp" "${extra[@]}"
  rm -rf "$dir" && mv "$dir.tmp" "$dir"
  echo "$coord" > "$dir/.coordinate"
  ls -l "$dir"
}

echo ">> Spark $SPARK_VERSION (minor $SPARK_MINOR, Scala $SCALA)"
fetch delta   "$SPARK_PROVIDED" "io.delta:delta-spark_${SPARK_MINOR}_${SCALA}:${DELTA_VERSION}"
fetch iceberg bundle            "org.apache.iceberg:iceberg-spark-runtime-${SPARK_MINOR}_${SCALA}:${ICEBERG_VERSION}"
fetch hudi    bundle            "org.apache.hudi:hudi-spark${SPARK_MINOR}-bundle_${SCALA}:${HUDI_VERSION}"
fetch kafka   "$KAFKA_PROVIDED" "org.apache.spark:spark-sql-kafka-0-10_${SCALA}:${SPARK_VERSION}"

chmod -R a+rX "$OUT"
echo ">> connector jars ready:"
find "$OUT" -name '*.jar' | sort
