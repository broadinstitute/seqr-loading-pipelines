#!/usr/bin/env python

import argparse
import getpass
import multiprocessing
import os
import socket

p = argparse.ArgumentParser()
p.add_argument("-c", "--cluster", default="no-vep")
p.add_argument("--run-locally", action="store_true", help="Run using a local hail install instead of submitting to dataproc. Assumes 'spark-submit' is on $PATH.")
p.add_argument("--spark-home", default=os.environ.get("SPARK_HOME"), help="The local spark directory (default: $SPARK_HOME). Required for --run-locally")
p.add_argument("--cpu-limit", help="How many CPUs to use when running locally. Defaults to all available CPUs.", type=int)
p.add_argument("--driver-memory", help="Spark driver memory limit when running locally", default="5G")
p.add_argument("--executor-memory", help="Spark executor memory limit when running locally", default="5G")
p.add_argument("--num-executors", help="Spark number of executors", default=str(multiprocessing.cpu_count()))
p.add_argument("script")

args, unparsed_args = p.parse_known_args()

#hail_zip = "gs://seqr-hail/hail-jar/hail-9-17-2018-f3e47061.zip"
#hail_jar = "gs://seqr-hail/hail-jar/hail-9-17-2018-f3e47061.jar"

hail_zip = "hail_builds/v01/hail-v01-10-8-2018-90c855449.zip"
hail_jar = "hail_builds/v01/hail-v01-10-8-2018-90c855449.jar"

script = args.script
script_args = " ".join(['"%s"' % arg for arg in unparsed_args])

if "load_dataset_to_es" in script:
    username = getpass.getuser()
    directory = "%s:%s" % (socket.gethostname(), os.getcwd())
    script_args += " --username '%(username)s' --directory '%(directory)s'" % locals()


if args.run_locally:
    if not args.spark_home:
        p.error("--spark-home is required with --run-locally")

    spark_home = args.spark_home
    cpu_limit_arg = ("--master local[%s]" % args.cpu_limit) if args.cpu_limit else ""
    driver_memory = args.driver_memory
    executor_memory = args.executor_memory
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
else:
    cluster = args.cluster

    hail_scripts_zip = "/tmp/hail_scripts.zip"

    os.chdir(os.path.join(os.path.dirname(__file__), ".."))
    os.system("zip -r %(hail_scripts_zip)s hail_scripts kubernetes download_and_create_reference_datasets/hail_scripts" % locals())

    command = """gcloud dataproc jobs submit pyspark \
      --cluster=%(cluster)s \
      --files=%(hail_jar)s \
      --py-files=%(hail_zip)s,%(hail_scripts_zip)s \
      --properties="spark.files=./$(basename %(hail_jar)s),spark.driver.extraClassPath=./$(basename %(hail_jar)s),spark.executor.extraClassPath=./$(basename %(hail_jar)s)" \
      "%(script)s" -- %(script_args)s
    """ % locals()

print(command)
os.system(command)
