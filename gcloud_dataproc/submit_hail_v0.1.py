#!/usr/bin/env python

import argparse
import os
import subprocess
import sys

p = argparse.ArgumentParser()
p.add_argument("-p", "--project", default="seqr-project")
p.add_argument("-c", "--cluster", default="no-vep")
p.add_argument("script")

args, unparsed_args = p.parse_known_args()

hash = subprocess.check_output("gsutil cat gs://hail-common/builds/0.1/latest-hash-spark-2.0.2.txt", shell=True).strip()
hail_zip="gs://hail-common/builds/0.1/python/hail-0.1-%(hash)s.zip" % locals()
hail_jar="gs://hail-common/builds/0.1/jars/hail-0.1-%(hash)s-Spark-2.0.2.jar" % locals()

project = args.project
cluster = args.cluster
script = args.script
script_args = " ".join(['"%s"' % arg for arg in unparsed_args])

utils_zip="/tmp/utils.zip"

os.system("zip -r %(utils_zip)s utils" % locals())

command = """gcloud dataproc jobs submit pyspark \
  --project=%(project)s \
  --cluster=%(cluster)s \
  --files=%(hail_jar)s \
  --py-files=%(hail_zip)s,%(utils_zip)s \
  --properties="spark.files=./$(basename %(hail_jar)s),spark.driver.extraClassPath=./$(basename %(hail_jar)s),spark.executor.extraClassPath=./$(basename %(hail_jar)s)" \
  "%(script)s" -- %(script_args)s
""" % locals()

print(command)
os.system(command)
