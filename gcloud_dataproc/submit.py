#!/usr/bin/env python

import argparse
import os
import subprocess


p = argparse.ArgumentParser()
p.add_argument("-c", "--cluster", default="no-vep")
p.add_argument("--spark-env")
p.add_argument("--region")
p.add_argument("--use-existing-scripts-zip", action="store_true")
p.add_argument("script")

args, unparsed_args = p.parse_known_args()

hail_zip = "hail_builds/v02/hail-0.2-3a68be23cb82d7c7fb5bf72668edcd1edf12822e.zip"
hail_jar = "hail_builds/v02/hail-0.2-3a68be23cb82d7c7fb5bf72668edcd1edf12822e-Spark-2.4.0.jar"

script = args.script
script_args = " ".join(['"%s"' % arg for arg in unparsed_args])

cluster = args.cluster
use_existing_script_zip = args.use_existing_scripts_zip

hail_scripts_zip = "/tmp/hail_scripts.zip"

os.chdir(os.path.join(os.path.dirname(__file__), ".."))
if use_existing_script_zip and os.path.exists(hail_scripts_zip):
    print('Using existing scripts zip file')
else:
    os.system("zip -r %(hail_scripts_zip)s hail_scripts kubernetes sv_pipeline download_and_create_reference_datasets/v02/hail_scripts" % locals())

properties_arg = ",".join([
    "spark.files=./$(basename %(hail_jar)s)",
    "spark.driver.extraClassPath=./$(basename %(hail_jar)s)",
    "spark.executor.extraClassPath=./$(basename %(hail_jar)s)",
]) % locals()
if args.spark_env:
    properties_arg += f',spark.executorEnv.{args.spark_env}'

region_arg = f'--region={args.region}' if args.region else ''
print(region_arg)
command = """gcloud dataproc jobs submit pyspark \
  --cluster=%(cluster)s \
  --files=%(hail_jar)s \
  --py-files=%(hail_zip)s,%(hail_scripts_zip)s \
  --properties="%(properties_arg)s" \
  %(region_arg)s \
  "%(script)s" -- %(script_args)s
""" % locals()

print(command)
subprocess.check_call(command, shell=True)
