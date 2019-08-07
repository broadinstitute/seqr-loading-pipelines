import argparse

from load_dataset import _get_es_node_settings, _set_k8s_context, _process_kubernetes_configs, _wait_for_data_nodes_state
from kubernetes.kubectl_utils import wait_until_pod_is_running
from kubernetes.shell_utils import run
from kubernetes.yaml_settings_utils import load_settings


p = argparse.ArgumentParser()
p.add_argument("--num-nodes", type=int, help="Number of es nodes to create.", default=3)
p.add_argument("--k8s-cluster-name", help="Specifies the kubernetes cluster name that hosts elasticsearch.", required=True)
args = p.parse_args()

settings = _get_es_node_settings(args.k8s_cluster_name, args.num_nodes)
load_settings([], settings)

# make sure cluster exists - create cluster with 1 node
run(" ".join([
    "gcloud container clusters create %(CLUSTER_NAME)s",
    "--machine-type %(CLUSTER_MACHINE_TYPE)s",
    "--num-nodes 1",   # "--scopes https://www.googleapis.com/auth/devstorage.read_write"
]) % settings)


_set_k8s_context(settings)

# create additional nodes
run(" ".join([
    "gcloud container node-pools create es-persistent-nodes",
    "--cluster %(CLUSTER_NAME)s",
    "--machine-type %(CLUSTER_MACHINE_TYPE)s",
    "--num-nodes " + str(int(settings.get("ES_DATA_NUM_PODS", 1)) - 1),
]) % settings)

# deploy elasticsearch
_process_kubernetes_configs("create", settings=settings,
    config_paths=[
        #"./gcloud_dataproc/utils/elasticsearch_cluster/es-configmap.yaml",
        "./kubernetes/elasticsearch-sharded/es-namespace.yaml",
        "./kubernetes/elasticsearch-sharded/es-discovery-svc.yaml",
        "./kubernetes/elasticsearch-sharded/es-master.yaml",
        "./kubernetes/elasticsearch-sharded/es-svc.yaml",
        "./kubernetes/elasticsearch-sharded/es-kibana.yaml",
    ])

wait_until_pod_is_running("es-kibana")

_process_kubernetes_configs("create", settings=settings,
    config_paths=[
        "./kubernetes/elasticsearch-sharded/es-client.yaml",
        "./kubernetes/elasticsearch-sharded/es-data-stateful.yaml",
        "./kubernetes/elasticsearch-sharded/es-data-svc.yaml",
    ])

_wait_for_data_nodes_state("create", settings, data_node_name="es-data")