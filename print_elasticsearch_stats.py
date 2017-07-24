import os
import pip

from utils.elasticsearch_utils import print_elasticsearch_stats

pip.main(['install', 'elasticsearch'])
import elasticsearch


# to get the ip address, run  `kubectl describe pod elasticsearch-1019229749-vhghc`
ELASTICSEARCH_HOST = os.environ.get('ELASTICSEARCH_SERVICE_HOST', "10.48.0.105")
ELASTICSEARCH_PORT = os.environ.get('ELASTICSEARCH_SERVICE_PORT', "30001") #"9200"
es = elasticsearch.Elasticsearch(ELASTICSEARCH_HOST, port=ELASTICSEARCH_PORT)

print_elasticsearch_stats(es)
