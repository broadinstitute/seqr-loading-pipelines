#!/usr/bin/env python

import argparse
import multiprocessing
import os
import subprocess

try:
    from pyspark.find_spark_home import _find_spark_home
    default_spark_home = _find_spark_home()
except:
    default_spark_home = os.environ.get("SPARK_HOME")

p = argparse.ArgumentParser()
p.add_argument("--spark-home", default=default_spark_home, help="The local spark directory (default: $SPARK_HOME). Required for --run-locally")
p.add_argument("--cpu-limit", help="How many CPUs to use when running locally. Defaults to all available CPUs.", type=int)
p.add_argument("--driver-memory", help="Spark driver memory limit when running locally")
p.add_argument("--executor-memory", help="Spark executor memory limit when running locally")
p.add_argument("--num-executors", help="Spark number of executors", default=str(multiprocessing.cpu_count()))
p.add_argument("script")

args, unparsed_args = p.parse_known_args()

hail_zip = "hail.zip"
hail_jar = "/usr/local/lib/python3.7/site-packages/hail/hail-all-spark.jar"

script = args.script
script_args = " ".join(['"%s"' % arg for arg in unparsed_args])

spark_home = args.spark_home
cpu_limit_arg = ("--master local[%s]" % args.cpu_limit) if args.cpu_limit else ""
driver_memory = args.driver_memory if args.driver_memory else "5G"
executor_memory = args.executor_memory if args.executor_memory else "5G"
num_executors = args.num_executors
command = """%(spark_home)s/bin/spark-submit \
    %(cpu_limit_arg)s \
    --driver-memory %(driver_memory)s \
    --executor-memory %(executor_memory)s \
    --num-executors %(num_executors)s \
    --conf spark.driver.extraJavaOptions=-Xss4M \
    --conf spark.executor.extraJavaOptions=-Xss4M \
    --conf spark.executor.memoryOverhead=5g \
    --conf spark.driver.maxResultSize=30g \
    --conf spark.kryoserializer.buffer.max=1g \
    --conf spark.memory.fraction=0.1 \
    --conf spark.default.parallelism=1 \
    --jars %(hail_jar)s \
    --conf spark.driver.extraClassPath=%(hail_jar)s \
    --conf spark.executor.extraClassPath=%(hail_jar)s \
    --py-files %(hail_zip)s \
    "%(script)s" %(script_args)s
""" % locals()

print(command)
subprocess.check_call(command, shell=True)
