import argparse
import pip

from utils.elasticsearch_utils import print_elasticsearch_stats

pip.main(['install', 'elasticsearch'])
import elasticsearch

p = argparse.ArgumentParser()
p.add_argument("-H", "--host", help="elasticsearch client host. The default address works if "
    "`kubectl proxy` is running in the background.",
    default="http://localhost:8001/api/v1/namespaces/default/services/elasticsearch:9200/proxy")
p.add_argument("-p", "--port", help="elasticsearch client port.", default="30001")

args = p.parse_args()

# to get the ip address, run  `kubectl describe pod elasticsearch-1019229749-vhghc`
ELASTICSEARCH_HOST = args.host
ELASTICSEARCH_PORT = args.port
es = elasticsearch.Elasticsearch(ELASTICSEARCH_HOST, port=ELASTICSEARCH_PORT)

#print_elasticsearch_stats(es)

print(es.cat.indices(h="index"))

#search(index="*coding*", body="""{
#
#}
#"""))
