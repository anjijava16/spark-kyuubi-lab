#!/bin/bash
# Downloads Delta Lake, Apache Iceberg and Apache Hudi jars that match the
# lab's Spark version into /opt/lakehouse/<format>/ (a shared Docker volume
# mounted read-only into kyuubi and the Spark containers).
#
# Why a volume instead of /opt/spark/jars: each format wants to own
# spark_catalog / session extensions, so putting all three on every job's
# classpath would make them fight. A job opts in to the format it needs by
# putting /opt/lakehouse/<format>/* on its driver+executor classpath (the
# --with flag of client/kyuubi_submit.py does that).
#
# Maven does the downloading, so transitive runtime deps (e.g. Delta's
# delta-storage) come along and a wrong version fails loudly here.
set -euo pipefail

OUT=/opt/lakehouse
SPARK_MINOR="${SPARK_VERSION%.*}"     # 4.1.3 -> 4.1
SCALA=2.13

fetch() {
  # fetch <dir-name> <transitive: yes|no> <group:artifact:version>
  local name=$1 transitive=$2 coord=$3
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
  if [[ "$transitive" == "no" ]]; then
    # shaded bundles: everything is already inside the one jar
    extra+=(-DexcludeTransitive=true)
  else
    # Spark, Scala, Hadoop and logging are already in the Spark image;
    # pulling second copies onto the classpath is how conflicts start.
    extra+=(-DexcludeGroupIds=org.apache.spark,org.scala-lang,org.scala-lang.modules,org.apache.hadoop,org.slf4j,org.apache.logging.log4j,com.fasterxml.jackson.core,com.fasterxml.jackson.module,org.antlr)
  fi
  mvn -B -q -f "$work/pom.xml" dependency:copy-dependencies \
      -DincludeScope=runtime -DoutputDirectory="$dir.tmp" "${extra[@]}"
  rm -rf "$dir" && mv "$dir.tmp" "$dir"
  echo "$coord" > "$dir/.coordinate"
  ls -l "$dir"
}

echo ">> Spark $SPARK_VERSION (minor $SPARK_MINOR, Scala $SCALA)"
fetch delta   yes "io.delta:delta-spark_${SPARK_MINOR}_${SCALA}:${DELTA_VERSION}"
fetch iceberg no  "org.apache.iceberg:iceberg-spark-runtime-${SPARK_MINOR}_${SCALA}:${ICEBERG_VERSION}"
fetch hudi    no  "org.apache.hudi:hudi-spark${SPARK_MINOR}-bundle_${SCALA}:${HUDI_VERSION}"

chmod -R a+rX "$OUT"
echo ">> lakehouse jars ready:"
find "$OUT" -name '*.jar' | sort
