#!/usr/bin/env python

import argparse
import os
import subprocess
import tempfile


def submit(script, script_args_list, cluster='no-vep', wait_for_job=True, use_existing_scripts_zip=False, region=None, spark_env=None, job_id=None):
    script_args = " ".join(['"%s"' % arg for arg in script_args_list])

    hail_scripts_zip =  os.path.join(tempfile.gettempdir(), 'hail_scripts.zip')

    os.chdir(os.path.join(os.path.dirname(__file__), ".."))
    if use_existing_scripts_zip and os.path.exists(hail_scripts_zip):
        print('Using existing scripts zip file')
    else:
        os.system(
            "zip -r %(hail_scripts_zip)s hail_scripts kubernetes sv_pipeline download_and_create_reference_datasets/v02/hail_scripts" % locals())

    command = f"""gcloud dataproc jobs submit pyspark \
      --cluster={cluster} \
      --py-files={hail_scripts_zip} \
      {f'--properties="spark.executorEnv.{spark_env}"' if spark_env else ''} \
      {f'--region={region}' if region else ''} \
      {f'--id={job_id}' if job_id else ''} \
      {'' if wait_for_job else '--async'} \
      "{script}" -- {script_args}
    """

    print(command)
    subprocess.check_call(command, shell=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument("-c", "--cluster", default="no-vep")
    p.add_argument("script")

    args, unparsed_args = p.parse_known_args()

    submit(args.script, unparsed_args, cluster=args.cluster)
