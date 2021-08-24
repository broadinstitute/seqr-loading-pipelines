#!/usr/bin/env python

import argparse
import logging
import os
import re
import time

from kubernetes.shell_utils import run
from kubernetes.kubectl_utils import is_pod_running, is_pod_not_running
from kubernetes.yaml_settings_utils import process_jinja_template, load_settings

logger = logging.getLogger()


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


def _compute_firewall_rule_name(k8s_cluster_name):
    return "%(k8s_cluster_name)s-firewall-rule" % locals()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--num-nodes", type=int, help="Number of es nodes to create.", default=3)
    p.add_argument("--k8s-cluster-name", help="Specifies the kubernetes cluster name that hosts elasticsearch.",
                   required=True)
    args = p.parse_args()

    settings = _get_es_node_settings(args.k8s_cluster_name, args.num_nodes)

    _create_es_nodes(settings)

