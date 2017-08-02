#!/usr/bin/env python

import argparse
import os

p = argparse.ArgumentParser()
p.add_argument("-c", "--cluster", default="dataproc-cluster-no-vep")
p.add_argument("script")

args, unknown_args = p.parse_known_args()

#hash = subprocess.check_output("gsutil cat gs://hail-common/latest-hash.txt", shell=True)
#HAIL_ZIP="gs://hail-common/pyhail-hail-is-master-${HASH}.zip"
#HAIL_JAR="gs://hail-common/hail-hail-is-master-all-spark2.0.2-${HASH}.jar"

cluster = args.cluster
script = args.script
script_args = " ".join(unknown_args)

hail_zip="gs://gnomad-bw2/hail-jar/hail-python.zip"
hail_jar="gs://gnomad-bw2/hail-jar/hail-all-spark.jar"

utils_zip="/tmp/utils.zip"

os.system("zip -r %(utils_zip)s utils" % locals())

command = """gcloud dataproc jobs submit pyspark \
  --cluster=%(cluster)s \
  --files=%(hail_jar)s \
  --py-files=%(hail_zip)s,%(utils_zip)s \
  --properties="spark.files=./$(basename %(hail_jar)s),spark.driver.extraClassPath=./$(basename %(hail_jar)s),spark.executor.extraClassPath=./$(basename %(hail_jar)s)" \
  "%(script)s" -- %(script_args)s
""" % locals()

print(command)
os.system(command)
