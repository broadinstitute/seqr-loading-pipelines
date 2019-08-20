#!/usr/bin/env python

import argparse
import getpass
import logging
import os
import random
import re
import sys
import time

from gcloud_dataproc.utils import seqr_api
from kubernetes.shell_utils import run
from kubernetes.kubectl_utils import is_pod_running, is_pod_not_running
from kubernetes.yaml_settings_utils import process_jinja_template, load_settings

logger = logging.getLogger()


def init_command_line_args():
    unique_id = random.randint(10**5, 10**6 - 1)
    random_dataproc_cluster_name = "vep-%s" % unique_id

    unique_id = random.randint(10**5, 10**6 - 1)
    random_es_cluster_name = "test-es-cluster-%s" % unique_id

    p = argparse.ArgumentParser()
    p.add_argument("-c", "--cluster-name", help="dataproc cluster name. If it doesn't exist, it will be created", default=random_dataproc_cluster_name)
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


    p.add_argument("--use-temp-loading-nodes", action="store_true",
        help="If specified, temporary loading nodes will be created and added to the elasticsearch cluster")
    p.add_argument("--k8s-cluster-name", help="Specifies the kubernetes cluster name that hosts elasticsearch (eg. 'gcloud-prod-es')."
        "This name is currently also re-used as elasticsearch's internal cluster name which it uses to link up with other elasticsearch instances.", default=random_es_cluster_name)
    p.add_argument("--num-temp-loading-nodes", type=int,
        help="For use with --num-temp-loading-nodes. Number of temp loading nodes to create.", default=3)

    p.add_argument("--host", help="Elastisearch host", default=os.environ.get("ELASTICSEARCH_SERVICE_HOSTNAME", "localhost"))
    p.add_argument("--port", help="Elastisearch port", default="9200")

    p.add_argument("input_dataset", help="input VCF or VDS")
    args, unparsed_args = p.parse_known_args()

    return args, unparsed_args


def submit_load_dataset_to_es_job(
        dataproc_cluster_name,
        start_with_step=0,
        stop_after_step=None,
        other_load_dataset_to_es_args=()):

    # submit job
    run(" ".join(map(str, [
        "./gcloud_dataproc/submit.py",
        "--hail-version 0.1",
        "--cluster %(dataproc_cluster_name)s",
        "hail_scripts/v01/load_dataset_to_es.py",
        "--stop-after-step %(stop_after_step)s " if stop_after_step is not None else "",
        "--start-with-step %(start_with_step)s ",
        ] + list(other_load_dataset_to_es_args))) % locals())


def _create_dataproc_cluster(dataproc_cluster_name, genome_version, num_workers=2, num_preemptible_workers=12):
    run("python ./gcloud_dataproc/v01/create_cluster_GRCh%(genome_version)s.py %(dataproc_cluster_name)s %(num_workers)s %(num_preemptible_workers)s" % locals(),
        errors_to_ignore=["Already exists"])


def _process_kubernetes_configs(action, config_paths, settings):
    for config_path in config_paths:
        # configure deployment dir
        output_dir = "/tmp/deployments/%(TIMESTAMP)s_%(CLUSTER_NAME)s" % settings
        process_jinja_template(".", config_path, settings, output_dir)

        config_path = os.path.join(output_dir, config_path)
        if action == "delete":
            run("kubectl delete -f %(config_path)s" % locals(), errors_to_ignore=["not found"])
        elif action == "create":
            run("kubectl apply -f %(config_path)s" % locals(), errors_to_ignore=["already exists", "already allocated"])


def _wait_for_data_nodes_state(action, settings, data_node_name="es-data-loading"):
    check_pod_state = is_pod_not_running if action == "delete" else is_pod_running
    # wait for all data nodes to enter desired state
    for i in range(int(settings.get("ES_DATA_NUM_PODS", 1))):
        done = False
        while not done:
            done = check_pod_state(data_node_name, pod_number=i)
            time.sleep(5)

def _set_k8s_context(settings):
    run("gcloud container clusters get-credentials %(CLUSTER_NAME)s" % settings)
    run("kubectl config set-context $(kubectl config current-context) --namespace=%(NAMESPACE)s" % settings)


def _create_temp_es_loading_nodes(settings):
    # make sure k8s cluster exists
    #run(" ".join([
    #    "gcloud container clusters create %(k8s_cluster_name)s",
    #    "--machine-type %(CLUSTER_MACHINE_TYPE)s",
    #    "--num-nodes 1",   # "--scopes https://www.googleapis.com/auth/devstorage.read_write"
    #]) % locals(), errors_to_ignore=["Already exists"])

    _set_k8s_context(settings)

    # add loading nodes
    run(" ".join([
        "gcloud container node-pools create loading-cluster ",
        "--cluster %(CLUSTER_NAME)s",
        "--machine-type %(CLUSTER_MACHINE_TYPE)s",
        "--num-nodes %(ES_DATA_NUM_PODS)s",
        "--local-ssd-count 1",
    ]) % settings, errors_to_ignore=["Already exists"])

    # deploy elasticsearch
    _process_kubernetes_configs("create", settings=settings,
        config_paths=[
            "./kubernetes/elasticsearch-sharded/es-data-stateless-local-ssd.yaml",
        ])

    _wait_for_data_nodes_state("create", settings)

    # get ip address of loading nodes
    elasticsearch_ip_address = run("kubectl get endpoints elasticsearch -o jsonpath='{.subsets[0].addresses[0].ip}'")

    logger.info("elasticsearch loading cluster IP address: {}".format(elasticsearch_ip_address))
    if not elasticsearch_ip_address or not re.match("\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", elasticsearch_ip_address):
        logger.error("Invalid elasticsearch IP address: '{}'".format(elasticsearch_ip_address))

    # add firewall rule to allow ingress
    firewall_rule_name = _compute_firewall_rule_name(settings["CLUSTER_NAME"])
    source_range = "%s.%s.0.0/16" % tuple(elasticsearch_ip_address.split(".")[0:2])
    for action in ["create", "update"]:
        run(("gcloud compute firewall-rules %(action)s %(firewall_rule_name)s "
             "--description='Allow any machine in the project-default network to connect to elasticsearch loading cluster ports 9200, 9300'"
             "--network=default "
             "--allow=tcp:9200,tcp:9300 "
             "--source-ranges=%(source_range)s ") % locals(), errors_to_ignore=["already exists"])

    return elasticsearch_ip_address


def _create_es_nodes(settings):
    logger.info("==> Create ES nodes")

    load_settings([], settings)

    ip_address = _create_temp_es_loading_nodes(settings)

    return ip_address


def _get_es_node_settings(k8s_cluster_name, num_temp_loading_nodes):
    return {
        "DEPLOY_TO": k8s_cluster_name,
        "CLUSTER_NAME": k8s_cluster_name,
        "ES_CLUSTER_NAME": k8s_cluster_name,
        "NAMESPACE": k8s_cluster_name,  # kubernetes namespace
        "IMAGE_PULL_POLICY": "Always",
        "TIMESTAMP": time.strftime("%Y%m%d_%H%M%S"),

        "CLUSTER_MACHINE_TYPE": "n1-highmem-4",
        "ELASTICSEARCH_VERSION": "6.3.2",
        "ELASTICSEARCH_JVM_MEMORY": "13g",
        "ELASTICSEARCH_DISK_SIZE": "100Gi",
        "ELASTICSEARCH_DISK_SNAPSHOTS": None,

        "KIBANA_SERVICE_PORT": 5601,

        "ES_CLIENT_NUM_PODS": 3,
        "ES_MASTER_NUM_PODS": 2,
        "ES_DATA_NUM_PODS": num_temp_loading_nodes,
    }


def _enable_cluster_routing_rebalance(enable, dataproc_cluster_name, host, port):
    logger.info("==> %s cluster.routing.rebalance", "enable" if enable else "disable")

    run(" ".join(map(str, [
        "./gcloud_dataproc/submit.py",
        "--hail-version 0.1",
        "--cluster", dataproc_cluster_name,
        "hail_scripts/elasticsearch_ops/cluster_routing_rebalance.py",
        "--host", host,
        "--port", port,
        "--enable" if enable else "--disable",
   ])))


def _compute_firewall_rule_name(k8s_cluster_name):
    return "%(k8s_cluster_name)s-firewall-rule" % locals()


def main():
    if "-h" in sys.argv or "--help" in sys.argv:
        run("python hail_scripts/v01/load_dataset_to_es.py -h")
        print("====================================================================================================")
        print("       NOTE: Any args not in the following list will be matched against args in the list above:")
        print("====================================================================================================")

    os.chdir(os.path.join(os.path.dirname(__file__), ".."))

    # get command-line args
    args, unparsed_args = init_command_line_args()

    # forward uparsed and other args to the load_dataset_to_es.py script
    load_dataset_to_es_args = unparsed_args

    load_dataset_to_es_args.extend([
        "--host", args.host,
        "--port", args.port,
        "--genome-version", args.genome_version,
        "--project-guid", args.project_guid,
        "--use-temp-loading-nodes" if args.use_temp_loading_nodes else "",
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
    if args.use_temp_loading_nodes and (args.stop_after_step == None or args.stop_after_step > 1):
        # make sure kubectl is installed
        run("kubectl version --client")


        # run vep and compute derived annotations before create temp elasticsearch loading nodes
        if args.start_with_step <= 1:
            # make sure cluster exists
            _create_dataproc_cluster(
                args.cluster_name,
                args.genome_version,
                num_workers=args.num_workers,
                num_preemptible_workers=args.num_preemptible_workers)
            submit_load_dataset_to_es_job(
                args.cluster_name,
                start_with_step=args.start_with_step,
                stop_after_step=1,
                other_load_dataset_to_es_args=load_dataset_to_es_args)

        # create temp es nodes
        settings = _get_es_node_settings(args.k8s_cluster_name, args.num_temp_loading_nodes)

        ip_address = _create_es_nodes(settings)

        # _enable_cluster_routing_rebalance(False, args.cluster_name, ip_address, args.port)

        # make sure cluster exists
        _create_dataproc_cluster(
            args.cluster_name,
            args.genome_version,
            num_workers=args.num_workers,
            num_preemptible_workers=args.num_preemptible_workers)

        # continue pipeline starting with loading steps, stream data to the new elasticsearch instance at ip_address
        submit_load_dataset_to_es_job(
            args.cluster_name,
            start_with_step=max(2, args.start_with_step),  # start with step 2 or later
            stop_after_step=args.stop_after_step,
            other_load_dataset_to_es_args=load_dataset_to_es_args + ["--host %(ip_address)s" % locals()])

        # _enable_cluster_routing_rebalance(True, args.cluster_name, ip_address, args.port)

    else:
        # make sure cluster exists
        _create_dataproc_cluster(
            args.cluster_name,
            args.genome_version,
            num_workers=args.num_workers,
            num_preemptible_workers=args.num_preemptible_workers)

        submit_load_dataset_to_es_job(
            args.cluster_name,
            start_with_step=args.start_with_step,
            stop_after_step=args.stop_after_step,
            other_load_dataset_to_es_args=load_dataset_to_es_args,
        )


if __name__ == "__main__":
    main()

