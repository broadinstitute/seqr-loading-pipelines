import os
os.system("pip install elasticsearch")

import argparse
from hail_scripts.v01.utils.elasticsearch_client import ElasticsearchClient

p = argparse.ArgumentParser()
p.add_argument("-H", "--host", help="elasticsearch client host. The default address works if "
    "`kubectl proxy` is running in the background.",
    default="http://localhost:8001/api/v1/namespaces/default/services/elasticsearch:9200/proxy")
p.add_argument("-p", "--port", help="elasticsearch client port.", default="30001")

args = p.parse_args()

# to get the ip address, run  `kubectl describe pod elasticsearch-1019229749-vhghc`
ELASTICSEARCH_HOST = args.host
ELASTICSEARCH_PORT = args.port

es = ElasticsearchClient(ELASTICSEARCH_HOST, port=ELASTICSEARCH_PORT)
es.print_elasticsearch_stats()



