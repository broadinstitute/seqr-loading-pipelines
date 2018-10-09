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
from kubernetes.shell_utils import run
from kubernetes.kubectl_utils import is_pod_running, wait_until_pod_is_running
from kubernetes.yaml_settings_utils import process_jinja_template, load_settings

logger = logging.getLogger()


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

    p.add_argument("--es-cluster-name", help="Specifies the elasticsearch cluster name.", default="es-cluster")
    p.add_argument("--num-persistent-es-nodes", help="If specified, a persistent ES cluster will be created first and "
        "this many persistent nodes will be be added to it before loading data or creating temp loading nodes.", type=int)

    p.add_argument("--use-temp-loading-nodes", help="Before loading the dataset, add temporary elasticsearch data "
        "nodes optimized for loading data, load the new dataset only to these nodes, then transfer the dataset to the "
        "persistent nodes.", action='store_true')
    p.add_argument("--delete-temp-loading-nodes-when-done", help="For use with --use-temp-loading-nodes. Delete the "
        "temp loading nodes when done.", action='store_true')
    p.add_argument("--num-temp-loading-nodes", help="For use with --use-temp-loading-nodes. Specifies the number of "
        "temporary loading nodes to add to the elasticsearch cluster", default=2, type=int)
    p.add_argument("--machine-type", help="For use with --use-temp-loading-nodes. Specifies the gcloud machine type of "
        "the extra loading nodes", default="n1-highmem-4", choices=MACHINE_TYPES)

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


def _process_kubernetes_configs(action, config_paths, template_variables):
    for config_path in config_paths:
        # configure deployment dir
        output_dir = "/tmp/deployments/%(TIMESTAMP)s_%(DEPLOY_TO)s" % template_variables
        process_jinja_template(".", config_path, template_variables, output_dir)

        config_path = os.path.join(output_dir, config_path)
        if action == "delete":
            run("kubectl delete -f %(config_path)s" % locals(), errors_to_ignore=["not found"])
        elif action == "create":
            run("kubectl create -f %(config_path)s" % locals(), errors_to_ignore=["already exists", "already allocated"])


def _create_persistent_es_nodes(machine_type, es_cluster_name, num_persistent_nodes=1, template_variables={}):
    # make sure cluster exists - create cluster with 1 node
    run(" ".join([
        "gcloud container clusters create %(es_cluster_name)s",
        "--machine-type %(machine_type)s",
        "--num-nodes 1",   # "--scopes https://www.googleapis.com/auth/devstorage.read_write"
    ]) % locals(), errors_to_ignore=["Already exists"])

    num_persistent_nodes -= 1

    run("time gcloud container clusters get-credentials %(es_cluster_name)s" % locals())

    if num_persistent_nodes > 0:
        # create additional nodes
        run(" ".join([
            "gcloud container node-pools create es-persistent-nodes",
            "--cluster %(es_cluster_name)s",
            "--machine-type %(machine_type)s",
            "--num-nodes %(num_persistent_nodes)s",
        ]) % locals(), errors_to_ignore=["Already exists"])

    # deploy elasticsearch
    for action in ["create"]:  # "delete",
        _process_kubernetes_configs(action, template_variables=template_variables,
            config_paths=[
                #"./gcloud_dataproc/utils/elasticsearch_cluster/es-configmap.yaml",
                "./kubernetes/elasticsearch-sharded/es-discovery-svc.yaml",
                "./kubernetes/elasticsearch-sharded/es-master.yaml",
                "./kubernetes/elasticsearch-sharded/es-svc.yaml",
                "./kubernetes/elasticsearch-sharded/es-kibana.yaml",
            ])

        if action == "create":
            wait_until_pod_is_running("kibana")

        _process_kubernetes_configs(action, template_variables=template_variables,
            config_paths=[
                "./kubernetes/elasticsearch-sharded/es-client.yaml",
                "./kubernetes/elasticsearch-sharded/es-data-stateful.yaml",
                "./kubernetes/elasticsearch-sharded/es-data-svc.yaml",
            ])

        # wait for all data nodes to enter desired state
        for i in range(int(template_variables.get("NUM_DATA_NODES", 1))):
            done = False
            while not done:
                done = is_pod_running("es-data", pod_number=i)
                if action == "delete":
                    done = not done
                time.sleep(5)



def _create_temp_es_loading_nodes(machine_type, es_cluster_name, num_loading_nodes=2, template_variables={}):
    # make sure k8s cluster exists
    run(" ".join([
        "gcloud container clusters create %(es_cluster_name)s",
        "--machine-type %(machine_type)s",
        "--num-nodes 1",   # "--scopes https://www.googleapis.com/auth/devstorage.read_write"
    ]) % locals(), errors_to_ignore=["Already exists"])

    run("time gcloud container clusters get-credentials %(es_cluster_name)s" % locals())

    # add loading nodes
    loading_node_pool_name = _compute_loading_pool_name(es_cluster_name)
    run(" ".join([
        "gcloud container node-pools create %(loading_node_pool_name)s",
        "--cluster %(es_cluster_name)s",
        "--machine-type %(machine_type)s",
        "--num-nodes %(num_loading_nodes)s",
        "--local-ssd-count 1",
    ]) % locals(), errors_to_ignore=["Already exists"])

    # deploy elasticsearch
    for action in ["create"]:  # ["delete"]:
        _process_kubernetes_configs(action, template_variables=template_variables,
            config_paths=[
                "./kubernetes/elasticsearch-sharded/es-data-stateful-local-ssd.yaml",
            ])

    # get ip address of loading nodes
    elasticsearch_ip_address = run("kubectl get endpoints elasticsearch -o jsonpath='{.subsets[0].addresses[0].ip}'")

    logger.info("elasticsearch loading cluster IP address: {}".format(elasticsearch_ip_address))
    if not elasticsearch_ip_address:
        logger.error("invalid elasticsearch loading cluster IP address: {}".format(elasticsearch_ip_address))

    # add firewall rule to allow ingress
    firewall_rule_name = _compute_firewall_rule_name(es_cluster_name)
    source_range = "%s.%s.0.0/16" % tuple(elasticsearch_ip_address.split(".")[0:2])
    for action in ["create", "update"]:
        run(("gcloud compute firewall-rules %(action)s %(firewall_rule_name)s "
             "--description='Allow any machine in the project-default network to connect to elasticsearch loading cluster ports 9200, 9300'"
             "--network=default "
             "--allow=tcp:9200,tcp:9300 "
             "--source-ranges=%(source_range)s ") % locals(), errors_to_ignore=["already exists"])

    return elasticsearch_ip_address


def _create_es_nodes(machine_type, es_cluster_name, num_persistent_es_nodes=0, num_temp_loading_nodes=2):
    settings = {
        "NAMESPACE": "default",
        "DEPLOY_TO": "elasticsearch-sharded-cluster",
        "IMAGE_PULL_POLICY": "Always",
        "ES_CLUSTER_NAME": es_cluster_name,  # "myesdb" or "es-cluster.."
        "ELASTICSEARCH_VERSION": "5.6.3",   # later 6.3.2
        "ES_CLIENT_NUM_PODS": 3,
        "ES_DATA_NUM_PODS": 2,
        "ES_MASTER_NUM_PODS": 2,
        "ELASTICSEARCH_JVM_MEMORY": "8g",
        "ELASTICSEARCH_DISK_SIZE": "15Gi",
    }
    load_settings([], settings)

    if num_persistent_es_nodes:
        _create_persistent_es_nodes(machine_type, es_cluster_name, num_persistent_nodes=num_persistent_es_nodes, template_variables=settings)

    ip_address = _create_temp_es_loading_nodes(machine_type, es_cluster_name, num_loading_nodes=num_temp_loading_nodes, template_variables=settings)

    return ip_address


def _compute_loading_pool_name(es_cluster_name):
    return "%(es_cluster_name)s-loading-cluster" % locals()


def _compute_firewall_rule_name(es_cluster_name):
    return "%(es_cluster_name)s-firewall-rule" % locals()


def _delete_temp_es_loading_nodes(es_cluster_name):
    loading_node_pool_name = _compute_loading_pool_name(es_cluster_name)
    run("echo Y | gcloud container node-pools delete --cluster %(es_cluster_name)s %(loading_node_pool_name)s" % locals())

    # delete firewall rule
    firewall_rule_name = _compute_firewall_rule_name(es_cluster_name)
    run("echo Y | gcloud compute firewall-rules delete %(firewall_rule_name)s" % locals())


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
    if not args.use_temp_loading_nodes:

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

        ip_address = _create_es_nodes(
            machine_type=args.machine_type,
            es_cluster_name=args.es_cluster_name,
            num_persistent_es_nodes=args.num_persistent_es_nodes,
            num_temp_loading_nodes=args.num_temp_loading_nodes)

        # continue pipeline starting with loading steps, stream data to the new elasticsearch instance at ip_address
        submit_load_dataset_to_es_job(
            args.genome_version,
            args.cluster_name,
            start_with_step=max(2, args.start_with_step),  # start with step 2 or later
            stop_after_step=args.stop_after_step,
            other_load_dataset_to_es_args=load_dataset_to_es_args + ["--host %(ip_address)s" % locals()],
            num_workers=args.num_workers,
            num_preemptible_workers=args.num_preemptible_workers)

        if args.delete_temp_loading_nodes_when_done:
            _delete_temp_es_loading_nodes(args.es_cluster_name)


if __name__ == "__main__":
    main()

