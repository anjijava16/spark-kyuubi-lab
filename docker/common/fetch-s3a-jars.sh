#!/bin/sh
# Downloads hadoop-aws + the matching AWS SDK bundle for whatever Hadoop
# version is sitting in a jar directory. Getting this pair wrong is the #1
# cause of ClassNotFound / NoSuchMethod errors with s3a, so we read the
# version instead of guessing it.
#
# usage: fetch-s3a-jars.sh <dir-with-hadoop-jars> <output-dir>
# env overrides: HADOOP_VERSION, AWS_SDK_VERSION, MAVEN_REPO
set -eu

SRC_DIR="$1"
OUT_DIR="$2"
REPO="${MAVEN_REPO:-https://repo1.maven.org/maven2}"
mkdir -p "$OUT_DIR"

HV="${HADOOP_VERSION:-}"
if [ -z "$HV" ]; then
  JAR=$(ls "$SRC_DIR" | grep -E '^hadoop-(client-api|common)-[0-9][0-9.]*\.jar$' | head -n1 || true)
  if [ -z "$JAR" ]; then
    echo "Could not find hadoop-client-api or hadoop-common jar in $SRC_DIR" >&2
    echo "Set HADOOP_VERSION as a build arg." >&2
    exit 1
  fi
  HV=$(echo "$JAR" | sed -E 's/^hadoop-(client-api|common)-([0-9.]+)\.jar$/\2/')
fi
echo ">> Hadoop version: $HV"

curl -fsSL -o "$OUT_DIR/hadoop-aws-$HV.jar" \
  "$REPO/org/apache/hadoop/hadoop-aws/$HV/hadoop-aws-$HV.jar"

POM=$(curl -fsSL "$REPO/org/apache/hadoop/hadoop-project/$HV/hadoop-project-$HV.pom")
V2=$(printf '%s\n' "$POM" | sed -n 's:.*<aws-java-sdk-v2.version>\([^<]*\)</aws-java-sdk-v2.version>.*:\1:p' | head -n1)
V1=$(printf '%s\n' "$POM" | sed -n 's:.*<aws-java-sdk.version>\([^<]*\)</aws-java-sdk.version>.*:\1:p' | head -n1)

if [ -n "${AWS_SDK_VERSION:-}" ]; then
  case "$AWS_SDK_VERSION" in 1.*) V1="$AWS_SDK_VERSION"; V2="" ;; *) V2="$AWS_SDK_VERSION" ;; esac
fi

if [ -n "$V2" ]; then
  echo ">> AWS SDK v2 bundle: $V2"
  curl -fsSL -o "$OUT_DIR/bundle-$V2.jar" \
    "$REPO/software/amazon/awssdk/bundle/$V2/bundle-$V2.jar"
elif [ -n "$V1" ]; then
  echo ">> AWS SDK v1 bundle: $V1"
  curl -fsSL -o "$OUT_DIR/aws-java-sdk-bundle-$V1.jar" \
    "$REPO/com/amazonaws/aws-java-sdk-bundle/$V1/aws-java-sdk-bundle-$V1.jar"
else
  echo "Could not read the AWS SDK version from hadoop-project-$HV.pom; set AWS_SDK_VERSION" >&2
  exit 1
fi

ls -l "$OUT_DIR"
