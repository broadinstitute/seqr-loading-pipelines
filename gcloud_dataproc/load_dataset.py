#!/usr/bin/env python

import argparse
import getpass
import logging
import os
import random
import sys
import time

from gcloud_dataproc.utils import seqr_api
from hail_scripts.v01.utils.shell_utils import run

logger = logging.getLogger()

if "-h" in sys.argv or "--help" in sys.argv:
    run("python hail_scripts/v01/load_dataset_to_es.py -h")
    sys.exit(0)

unique_id = random.randint(10**5, 10**6 - 1)
random_cluster_name = "vep-%s" % unique_id

p = argparse.ArgumentParser()
p.add_argument("-c", "--cluster-name", help="dataproc cluster name. If it doesn't exist, it will be created", default=random_cluster_name)
p.add_argument("--genome-version", help="genome version: 37 or 38", choices=["37", "38"], required=True)
p.add_argument("--project-guid", help="seqr project guid", required=True)

p.add_argument("--seqr-url", help="seqr url for retrieving pedigree info", default="https://seqr.broadinstitute.org")
p.add_argument("--seqr-username", help="seqr username for retrieving pedigree info")
p.add_argument("--seqr-password", help="seqr password for retrieving pedigree info")

p.add_argument("--start-with-step", help="which pipeline step to start with.", type=int, default=0, choices=[0, 1, 2, 3, 4])
p.add_argument("--download-fam-file", help="download .fam file from seqr", action='store_true')
p.add_argument("--use-temp-es-cluster", help="create a temporary elasticsearch cluster for loading", action='store_true')
args, unparsed_args = p.parse_known_args()

os.chdir(os.path.join(os.path.dirname(__file__), ".."))

# forward uparsed and other args to the load_dataset_to_es.py script
load_dataset_to_es_args = unparsed_args
load_dataset_to_es_args.extend([
    "--genome-version", args.genome_version,
    "--project-guid", args.project_guid
])

# download .fam file?
is_fam_file_specified = "--fam-file" in unparsed_args
is_subset_samples_file_specified = "--subset-samples" in unparsed_args

if args.download_fam_file and (not is_fam_file_specified or not is_subset_samples_file_specified):
    # prompt for seqr username and password
    seqr_username = args.seqr_username or input("seqr username: ")
    seqr_password = args.seqr_password or getpass.getpass("seqr password: ")

    # download file
    fam_file_path, subset_samples_file_path = seqr_api.download_pedigree_info(
        args.project_guid, seqr_username=seqr_username, seqr_password=seqr_password)

    # find input vcf file to get the vcf_directory
    for unparsed_arg in unparsed_args:
        if not unparsed_arg.startswith("gs://"):
            continue
        if ".vcf" in unparsed_arg or unparsed_arg.endswith(".vds"):
            vcf_directory = os.path.dirname(unparsed_arg) or "."
            break
    else:
        raise ValueError("gs:// path not .vcf or .vds file not found in args")

    # upload fam file to vcf_directory
    if not is_fam_file_specified:
        fam_file_gcloud_path = os.path.join(vcf_directory, os.path.basename(fam_file_path))
        run("gsutil cp %(fam_file_path)s %(fam_file_gcloud_path)s" % locals())
        load_dataset_to_es_args.extend(["--fam-file", fam_file_gcloud_path])

    # upload subset-samples to vcf_directory
    if not is_subset_samples_file_specified:
        subset_samples_file_gcloud_path = os.path.join(vcf_directory, os.path.basename(subset_samples_file_path))
        run("gsutil cp %(subset_samples_file_path)s %(subset_samples_file_gcloud_path)s" % locals())
        load_dataset_to_es_args.extend(["--subset-samples", subset_samples_file_gcloud_path])


genome_version = args.genome_version
dataproc_cluster_name = args.cluster_name

run((
    "python ./gcloud_dataproc/create_cluster_GRCh%(genome_version)s.py "
    "--project=seqr-project "
    "%(dataproc_cluster_name)s 2 24") % locals(), errors_to_ignore=["Already exists"])

if not args.use_temp_es_cluster:
    load_dataset_to_es_args.extend([
        "--start-with-step", args.start_with_step,
    ])
    run((
        "time ./gcloud_dataproc/submit.py "
        "--cluster %(dataproc_cluster_name)s "
        "--project seqr-project "
        "hail_scripts/v01/load_dataset_to_es.py " +
        " ".join(map(str, load_dataset_to_es_args))
    ) % locals())
else:
    # make sure kubectl is installed
    run("kubectl version --client")

    # run vep and compute derived annotations
    if args.start_with_step <= 1:
        run((
            "time ./gcloud_dataproc/submit.py "
            "--cluster %(dataproc_cluster_name)s "
            "hail_scripts/v01/load_dataset_to_es.py " +
            " ".join(map(str, load_dataset_to_es_args)) + " "
            "--stop-after-step 1 "
        ) % locals())

    # create elasticsearch nodes for loading
    elasticsearch_cluster_name = "es-loading-cluster"
    elasticsearch_cluster_firewall_rule_name = "es-loading-cluster-allow-all"

    # delete cluster in case it already exists
    #run("echo Y | gcloud compute firewall-rules delete %(elasticsearch_cluster_firewall_rule_name)s" % locals(), errors_to_ignore=["not found"])
    #run("echo Y | gcloud container clusters delete %(elasticsearch_cluster_name)s" % locals(), errors_to_ignore=["not found"])

    # create cluster
    try:
        run((
            "gcloud container clusters create %(elasticsearch_cluster_name)s "
            "--machine-type n1-highmem-4 "
            "--local-ssd-count 1 "
            "--num-nodes 2"   # "--scopes https://www.googleapis.com/auth/devstorage.read_write"
            ) % locals(), errors_to_ignore=["Already exists"])

        run("time gcloud container clusters get-credentials %(elasticsearch_cluster_name)s" % locals())

        for action in ["delete", "create"]:
            continue
            for config_path in [
                #"./gcloud_dataproc/utils/elasticsearch_cluster/es-configmap.yaml",
                "./gcloud_dataproc/utils/elasticsearch_cluster/es-discovery-svc.yaml",
                "./gcloud_dataproc/utils/elasticsearch_cluster/es-master.yaml",
                "./gcloud_dataproc/utils/elasticsearch_cluster/es-svc.yaml",
                "./gcloud_dataproc/utils/elasticsearch_cluster/es-kibana.yaml",
                "--- sleep ---",  # gives pods time to initialize
                "./gcloud_dataproc/utils/elasticsearch_cluster/es-client.yaml",
                "./gcloud_dataproc/utils/elasticsearch_cluster/es-data-stateful-local-ssd.yaml",
                "./gcloud_dataproc/utils/elasticsearch_cluster/es-data-svc.yaml",
                "--- sleep ---",  # gives pods time to initialize
            ]:
                if action == "delete":
                    if not config_path.startswith("---"):
                        run("kubectl delete -f %(config_path)s" % locals(), errors_to_ignore=["not found"])
                elif action == "create":
                    if config_path == "--- sleep ---":
                        SLEEP_SECONDS = 20
                        logger.info("Wait for %s seconds" % SLEEP_SECONDS)
                        time.sleep(SLEEP_SECONDS)
                    else:
                        run("kubectl %(action)s -f %(config_path)s" % locals(), errors_to_ignore=["already exists", "already allocated"])

        # get ip address of loading nodes
        ip_address = run("kubectl get endpoints elasticsearch -o jsonpath='{.subsets[0].addresses[0].ip}'")

        logger.info("elasticsearch loading cluster IP address: %(ip_address)s" % locals())
        if not ip_address:
            logger.error("invalid elasticsearch loading cluster IP address")

        # add firewall rule to allow ingress
        source_range = "%s.%s.0.0/16" % tuple(ip_address.split(".")[0:2])
        for action in ["create", "update"]:
                run(("gcloud compute firewall-rules %(action)s %(elasticsearch_cluster_firewall_rule_name)s "
                "--description='Allow any machine in the project-default network to connect to elasticsearch loading cluster ports 9200, 9300'"
                "--network=default "
                "--allow=tcp:9200,tcp:9300 "
                "--source-ranges=%(source_range)s ") % locals(), errors_to_ignore=["already exists"])

        # run pipeline loading steps, stream data to the new elasticsearch instance at ip_address
        start_with_step = max(args.start_with_step, 2)
        run((
            "time ./gcloud_dataproc/submit.py "
            "--cluster %(dataproc_cluster_name)s "
            "--project seqr-project "
            "hail_scripts/v01/load_dataset_to_es.py " +
            " ".join(map(str, load_dataset_to_es_args)) + " " +
            "--start-with-step %(start_with_step)s "
            "--host %(ip_address)s"
        ) % locals())

    finally:
        logger.info("Deleting resources")

        #run("echo Y | gcloud compute firewall-rules delete %(elasticsearch_cluster_firewall_rule_name)s" % locals())
        #run("echo Y | gcloud container clusters delete %(elasticsearch_cluster_name)s" % locals())



    # transfer to existing cluster

    """
    # disable shard reallocation
    curl -XPUT http://my.els.clust.er/_cluster/settings -d '{
        "transient" : {
            "cluster.routing.allocation.enable" : "none"
        }
    }'
    """


    """
    # enable shard reallocation
    curl -XPUT http://my.els.clust.er/_cluster/settings -d '{
        "transient" : {
            "cluster.routing.allocation.enable" : "all"
        }
    }'
    """

    # use shard allocation filtering (https://www.elastic.co/guide/en/elasticsearch/reference/current/shard-allocation-filtering.html)
    # mark nodes as no index
    # delete cluster
