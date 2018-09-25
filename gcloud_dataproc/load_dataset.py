#!/usr/bin/env python

import argparse
import getpass
import logging
import os
import random
import sys
import time

from gcloud_dataproc.utils import seqr_api
from gcloud_dataproc.utils.machine_types import MACHINE_TYPES
from hail_scripts.v01.utils.shell_utils import run

logger = logging.getLogger()

ELASTICSEARCH_CLUSTER_NAME = "es-cluster"
LOADING_NODE_POOL_NAME = "es-loading-nodes"
FIREWALL_RULE_NAME = "es-loading-cluster-allow-all"
SLEEP_SECONDS = 20


def init_command_line_args():
    unique_id = random.randint(10**5, 10**6 - 1)
    random_cluster_name = "vep-%s" % unique_id

    p = argparse.ArgumentParser()
    p.add_argument("-c", "--cluster-name", help="dataproc cluster name. If it doesn't exist, it will be created", default=random_cluster_name)
    p.add_argument("--num-workers", help="num dataproc worker nodes to create", default=2, type=int)
    p.add_argument("--num-preemptible-workers", help="num preemptible dataproc worker nodes to create", default=12, type=int)

    p.add_argument("--genome-version", help="genome version: 37 or 38", choices=["37", "38"], required=True)
    p.add_argument("--project-guid", help="seqr project guid", required=True)

    p.add_argument("--seqr-url", help="seqr url for retrieving pedigree info", default="https://seqr.broadinstitute.org")
    p.add_argument("--seqr-username", help="seqr username for retrieving pedigree info")
    p.add_argument("--seqr-password", help="seqr password for retrieving pedigree info")

    p.add_argument("--start-with-step", help="which pipeline step to start with.", type=int, default=0, choices=[0, 1, 2, 3, 4])
    p.add_argument("--stop-after-step", help="stop after this pipeline step", type=int)
    p.add_argument("--download-fam-file", help="download .fam file from seqr", action='store_true')

    p.add_argument("--use-temp-es-cluster", help="Before loading the dataset, add temporary elasticsearch data nodes optimized "
        "for loading data, load the new dataset only to these nodes, then transfer the dataset to the persistent nodes and "
        "delete the temporary nodes.", action='store_true')
    p.add_argument("--machine-type", help="For --use-temp-es-cluster, specifies the gcloud machine type of the extra loading nodes", default="n1-highmem-4",
                   choices=MACHINE_TYPES)
    p.add_argument("--num-loading-nodes", help="For --use-temp-es-cluster, specifies the number of temporary loading nodes to add to the elasticsearch cluster", default=2, type=int)

    p.add_argument("input_dataset", help="input VCF or VDS")
    args, unparsed_args = p.parse_known_args()

    return args, unparsed_args


def submit_load_dataset_to_es_job(genome_version, dataproc_cluster_name, start_with_step=0, stop_after_step=None, other_load_dataset_to_es_args=(),
    num_workers=2, num_preemptible_workers=12):

    # make sure the dataproc cluster exists
    run("python ./gcloud_dataproc/create_cluster_GRCh%(genome_version)s.py --project=seqr-project %(dataproc_cluster_name)s %(num_workers)s %(num_preemptible_workers)s" % locals(),
        errors_to_ignore=["Already exists"])

    # submit job
    run(" ".join(map(str, [
        "time ./gcloud_dataproc/submit.py",
        "--cluster %(dataproc_cluster_name)s",
        "hail_scripts/v01/load_dataset_to_es.py",
        "--stop-after-step %(stop_after_step)s " if stop_after_step is not None else "",
        "--start-with-step %(start_with_step)s ",
        ] + list(other_load_dataset_to_es_args))) % locals())


def create_persistent_es_nodes(machine_type="n1-highmem-4"):
    # NOTE: this function isn't used currently - assumes persistent nodes get created elsewhere

    params = dict(globals())
    params.update(locals())

    # delete cluster in case it already exists
    #run("echo Y | gcloud compute firewall-rules delete %(FIREWALL_RULE_NAME)s" % params, errors_to_ignore=["not found"])
    #run("echo Y | gcloud container clusters delete %(ELASTICSEARCH_CLUSTER_NAME)s" % params, errors_to_ignore=["not found"])

    # make sure cluster exists
    run(" ".join([
        "gcloud container clusters create %(ELASTICSEARCH_CLUSTER_NAME)s",
        "--machine-type %(machine_type)s",
        "--num-nodes 1",   # "--scopes https://www.googleapis.com/auth/devstorage.read_write"
    ]) % params, errors_to_ignore=["Already exists"])

    run("time gcloud container clusters get-credentials %(ELASTICSEARCH_CLUSTER_NAME)s" % params)

    run(" ".join([
        "gcloud container node-pools create es-persistent-nodes",
        "--cluster %(ELASTICSEARCH_CLUSTER_NAME)s",
        "--machine-type %(machine_type)s",
        "--num-nodes 2",
    ]) % params, errors_to_ignore=["Already exists"])

    # deploy elasticsearch
    for action in ["create"]:  # "delete",

        for config_path in [
            #"./gcloud_dataproc/utils/elasticsearch_cluster/es-configmap.yaml",
            "./gcloud_dataproc/utils/elasticsearch_cluster/es-discovery-svc.yaml",
            "./gcloud_dataproc/utils/elasticsearch_cluster/es-master.yaml",
            "./gcloud_dataproc/utils/elasticsearch_cluster/es-svc.yaml",
            "./gcloud_dataproc/utils/elasticsearch_cluster/es-kibana.yaml",
            "--- sleep ---",  # gives pods time to initialize
            "./gcloud_dataproc/utils/elasticsearch_cluster/es-client.yaml",
            "./gcloud_dataproc/utils/elasticsearch_cluster/es-data-stateful.yaml",
            "./gcloud_dataproc/utils/elasticsearch_cluster/es-data-svc.yaml",
            "--- sleep ---",  # gives pods time to initialize
        ]:
            if action == "delete":
                if not config_path.startswith("---"):
                    run("kubectl delete -f %(config_path)s" % locals(), errors_to_ignore=["not found"])
            elif action == "create":
                if config_path == "--- sleep ---":
                    logger.info("Wait for %s seconds" % SLEEP_SECONDS)
                    time.sleep(SLEEP_SECONDS)
                else:
                    run("kubectl create -f %(config_path)s" % locals(), errors_to_ignore=["already exists", "already allocated"])

        if action == "delete":
            status = "."
            while status is not None and status.strip():
                status = run("kubectl get pods -l component=elasticsearch -o jsonpath='{.items[*].status.phase}'")
                logger.info("Waiting for components to terminate: %s" % (status,))
                time.sleep(5)


def create_temp_es_loading_nodes(machine_type="n1-highmem-4", num_loading_nodes=2):
    params = dict(globals())
    params.update(locals())

    # delete cluster in case it already exists
    #run("echo Y | gcloud compute firewall-rules delete %(FIREWALL_RULE_NAME)s" % params, errors_to_ignore=["not found"])
    #run("echo Y | gcloud container clusters delete %(ELASTICSEARCH_CLUSTER_NAME)s" % params, errors_to_ignore=["not found"])

    # make sure k8s cluster exists
    run(" ".join([
        "gcloud container clusters create %(ELASTICSEARCH_CLUSTER_NAME)s",
        "--machine-type %(machine_type)s",
        "--num-nodes 1",   # "--scopes https://www.googleapis.com/auth/devstorage.read_write"
    ]) % params, errors_to_ignore=["Already exists"])

    run("time gcloud container clusters get-credentials %(ELASTICSEARCH_CLUSTER_NAME)s" % params)

    # add loading nodes
    run(" ".join([
        "gcloud container node-pools create %(LOADING_NODE_POOL_NAME)s",
        "--cluster %(ELASTICSEARCH_CLUSTER_NAME)s",
        "--machine-type %(machine_type)s",
        "--num-nodes %(num_loading_nodes)s",
        "--local-ssd-count 1",
    ]) % params, errors_to_ignore=["Already exists"])

    # deploy elasticsearch
    for action in ["create"]:  # ["delete"]:

        for config_path in [
            "./gcloud_dataproc/utils/elasticsearch_cluster/es-data-stateful-local-ssd.yaml",  # TODO pass num_loading_nodes into the template
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
                    run("kubectl create -f %(config_path)s" % locals(), errors_to_ignore=["already exists", "already allocated"])

        if action == "delete":
            status = "."
            while status is not None and status.strip():
                status = run("kubectl get pods -l component=elasticsearch -o jsonpath='{.items[*].status.phase}'")
                logger.info("Waiting for components to terminate: %s" % (status,))
                time.sleep(5)

    # get ip address of loading nodes
    ip_address = run("kubectl get endpoints elasticsearch -o jsonpath='{.subsets[0].addresses[0].ip}'")

    logger.info("elasticsearch loading cluster IP address: {}".format(ip_address))
    if not ip_address:
        logger.error("invalid elasticsearch loading cluster IP address: {}".format(ip_address))

    # add firewall rule to allow ingress
    source_range = "%s.%s.0.0/16" % tuple(ip_address.split(".")[0:2])
    params.update(locals())
    for action in ["create", "update"]:
        run(("gcloud compute firewall-rules %(action)s %(FIREWALL_RULE_NAME)s "
             "--description='Allow any machine in the project-default network to connect to elasticsearch loading cluster ports 9200, 9300'"
             "--network=default "
             "--allow=tcp:9200,tcp:9300 "
             "--source-ranges=%(source_range)s ") % params, errors_to_ignore=["already exists"])

    return ip_address


def delete_temp_es_loading_nodes():
    params = dict(globals())
    params.update(locals())

    run("echo Y | gcloud container node-pools delete --cluster %(ELASTICSEARCH_CLUSTER_NAME)s %(LOADING_NODE_POOL_NAME)s" % params)


def main():
    if "-h" in sys.argv or "--help" in sys.argv:
        run("python hail_scripts/v01/load_dataset_to_es.py -h")
        sys.exit(0)

    os.chdir(os.path.join(os.path.dirname(__file__), ".."))

    # get command-line args
    args, unparsed_args = init_command_line_args()

    # forward uparsed and other args to the load_dataset_to_es.py script
    load_dataset_to_es_args = unparsed_args

    load_dataset_to_es_args.extend([
        "--genome-version", args.genome_version,
        "--project-guid", args.project_guid,
        "--use-temp-es-cluster" if args.use_temp_es_cluster else "",
        args.input_dataset,
    ])

    # download .fam file?
    is_fam_file_specified = "--fam-file" in unparsed_args
    is_subset_samples_file_specified = "--subset-samples" in unparsed_args

    if args.download_fam_file and (not is_fam_file_specified or not is_subset_samples_file_specified):
        input_dataset_directory = os.path.dirname(args.input_dataset) or "."

        # prompt for seqr username and password
        seqr_username = args.seqr_username or input("seqr username: ")
        seqr_password = args.seqr_password or getpass.getpass("seqr password: ")

        # download file
        fam_file_path, subset_samples_file_path = seqr_api.download_pedigree_info(
            args.project_guid, seqr_username=seqr_username, seqr_password=seqr_password)

        # upload fam file to vcf_directory
        if not is_fam_file_specified:
            fam_file_gcloud_path = os.path.join(input_dataset_directory, os.path.basename(fam_file_path))
            run("gsutil cp %(fam_file_path)s %(fam_file_gcloud_path)s" % locals())
            load_dataset_to_es_args.extend(["--fam-file", fam_file_gcloud_path])

        # upload subset-samples to vcf_directory
        if not is_subset_samples_file_specified:
            subset_samples_file_gcloud_path = os.path.join(input_dataset_directory, os.path.basename(subset_samples_file_path))
            run("gsutil cp %(subset_samples_file_path)s %(subset_samples_file_gcloud_path)s" % locals())
            load_dataset_to_es_args.extend(["--subset-samples", subset_samples_file_gcloud_path])

    # run pipeline with or without using a temp elasticsearch cluster for loading
    if not args.use_temp_es_cluster:

        submit_load_dataset_to_es_job(
            args.genome_version,
            args.cluster_name,
            start_with_step=args.start_with_step,
            stop_after_step=args.stop_after_step,
            other_load_dataset_to_es_args=load_dataset_to_es_args,
        )

    else:
        # make sure kubectl is installed
        run("kubectl version --client")

        # run vep and compute derived annotations
        if args.start_with_step <= 1:
            submit_load_dataset_to_es_job(
                args.genome_version,
                args.cluster_name,
                start_with_step=args.start_with_step,
                stop_after_step=1,
                other_load_dataset_to_es_args=load_dataset_to_es_args)

        ip_address = create_temp_es_loading_nodes(machine_type=args.machine_type, num_loading_nodes=args.num_loading_nodes)

        # continue pipeline starting with loading steps, stream data to the new elasticsearch instance at ip_address
        submit_load_dataset_to_es_job(
            args.genome_version,
            args.cluster_name,
            start_with_step=max(2, args.start_with_step),  # start with step 2 or later
            stop_after_step=args.stop_after_step,
            other_load_dataset_to_es_args=load_dataset_to_es_args + ["--host %(ip_address)s" % locals()],
            num_workers=args.num_workers,
            num_preemptible_workers=args.num_preemptible_workers,
        )

        delete_temp_es_loading_nodes()


        #run("echo Y | gcloud compute firewall-rules delete %(FIREWALL_RULE_NAME)s" % locals())
        #run("echo Y | gcloud container clusters delete %(ELASTICSEARCH_CLUSTER_NAME)s" % locals())


        # use shard allocation filtering (https://www.elastic.co/guide/en/elasticsearch/reference/current/shard-allocation-filtering.html)
        # delete cluster


if __name__ == "__main__":
    main()

