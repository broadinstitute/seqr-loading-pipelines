import argparse
import os

from kubernetes.shell_utils import run
from load_dataset import _get_es_node_settings, _process_kubernetes_configs, _wait_for_data_nodes_state, \
    _compute_firewall_rule_name, _set_k8s_context
from hail_scripts.v01.utils.elasticsearch_utils import wait_for_loading_shards_transfer
from hail_scripts.v01.utils.elasticsearch_client import ElasticsearchClient

p = argparse.ArgumentParser()
p.add_argument("--num-temp-loading-nodes", type=int, help="For use with --num-temp-loading-nodes. Number of temp loading nodes to create.", default=3)
p.add_argument("--host", help="Elastisearch host", default=os.environ.get("ELASTICSEARCH_SERVICE_HOSTNAME", "localhost"))
p.add_argument("--port", help="Elastisearch port", default="9200")
p.add_argument("--k8s-cluster-name", help="Specifies the kubernetes cluster name that hosts elasticsearch.", required=True)
args = p.parse_args()

client = ElasticsearchClient(args.host, args.port)
wait_for_loading_shards_transfer(client, num_attempts=1)

settings = _get_es_node_settings(args.k8s_cluster_name, args.num_temp_loading_nodes)
_set_k8s_context(settings)

_process_kubernetes_configs("delete", settings=settings,
    config_paths=[
        "./kubernetes/elasticsearch-sharded/es-data-stateless-local-ssd.yaml",
    ])
_wait_for_data_nodes_state("delete", settings)

run("echo Y | gcloud container node-pools delete --cluster {} loading-cluster".format(args.k8s_cluster_name))

# delete firewall rule
firewall_rule_name = _compute_firewall_rule_name(args.k8s_cluster_name)
run("echo Y | gcloud compute firewall-rules delete {}s".format(firewall_rule_name))
