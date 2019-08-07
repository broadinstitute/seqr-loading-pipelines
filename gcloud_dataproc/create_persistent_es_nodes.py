import argparse

from load_dataset import _get_es_node_settings, _create_persistent_es_nodes
from kubernetes.yaml_settings_utils import load_settings


p = argparse.ArgumentParser()
p.add_argument("--num-nodes", type=int, help="Number of es nodes to create.", default=3)
p.add_argument("--k8s-cluster-name", help="Specifies the kubernetes cluster name that hosts elasticsearch.", required=True)
args = p.parse_args()

settings = _get_es_node_settings(args.k8s_cluster_name, args.num_nodes)
load_settings([], settings)
_create_persistent_es_nodes(settings)