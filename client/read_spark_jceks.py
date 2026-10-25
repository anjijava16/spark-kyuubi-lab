"""
create_jceks.py - build a Hadoop-compatible .jceks credential file using only
pip-installed pyspark (no Hadoop install, no keytool, no password to remember).

The file is written with Hadoop's default provider password ("none"), so Spark
can read it later with hadoopConfiguration().getPassword(alias) and you never
pass a password to spark-submit.

Usage:
    python create_jceks.py --out ./db.jceks --alias teradata.password --alias sqlserver.password
        -> prompts for each value (hidden input)

    python create_jceks.py --out ./db.jceks --from-env TD_PASS:teradata.password
        -> reads the value from env var TD_PASS (handy in CI)

Running it again on the same file adds new aliases / overwrites existing ones.
"""
import argparse
import getpass
import os
import sys

from pyspark.sql import SparkSession


def build_spark():
    # Small local session, just to get at the Hadoop classes bundled with pyspark
    return (SparkSession.builder
            .master("local[1]")
            .appName("create-jceks")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def to_char_array(spark, value):
    # py4j turns Java Strings into Python str, so build the Java char[] by hand
    gateway = spark.sparkContext._gateway
    arr = gateway.new_array(gateway.jvm.char, len(value))
    for i, ch in enumerate(value):
        arr[i] = ch
    return arr


def get_provider(spark, jceks_file):
    jvm = spark._jvm
    conf = jvm.org.apache.hadoop.conf.Configuration()
    conf.set("hadoop.security.credential.provider.path", "jceks://file" + jceks_file)
    providers = jvm.org.apache.hadoop.security.alias.CredentialProviderFactory.getProviders(conf)
    return providers.get(0)


def main():
    p = argparse.ArgumentParser(description="Create/update a Hadoop .jceks credential file")
    p.add_argument("--out", required=True, help="path of the .jceks file to create/update")
    p.add_argument("--alias", action="append", default=[],
                   help="alias to store; you will be prompted for its value (repeatable)")
    p.add_argument("--from-env", action="append", default=[], metavar="ENV_VAR:alias",
                   help="take the value for alias from an environment variable (repeatable)")
    args = p.parse_args()

    if os.environ.get("HADOOP_CREDSTORE_PASSWORD"):
        sys.exit("HADOOP_CREDSTORE_PASSWORD is set. Unset it, otherwise the job will need "
                 "the same password to read the file.")

    secrets = {}
    for alias in args.alias:
        value = getpass.getpass(f"Value for {alias}: ")
        if not value:
            sys.exit(f"empty value for {alias}")
        secrets[alias.lower()] = value
    for item in args.from_env:
        env_var, _, alias = item.partition(":")
        if not alias or env_var not in os.environ:
            sys.exit(f"bad --from-env '{item}' (format ENV_VAR:alias, and the env var must exist)")
        secrets[alias.lower()] = os.environ[env_var]

    if not secrets:
        sys.exit("nothing to store - pass at least one --alias or --from-env")

    jceks_file = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(jceks_file), exist_ok=True)

    spark = build_spark()
    try:
        jvm = spark._jvm
        provider = get_provider(spark, jceks_file)
        existing = set(provider.getAliases())

        for alias, value in secrets.items():
            chars = to_char_array(spark, value)
            if alias in existing:
                provider.deleteCredentialEntry(alias)
                print(f"updated : {alias}")
            else:
                print(f"added   : {alias}")
            provider.createCredentialEntry(alias, chars)

        provider.flush()
        os.chmod(jceks_file, 0o600)   # only you can read it locally
        print(f"\nwrote {jceks_file}")
        print("aliases in file:", sorted(provider.getAliases()))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
