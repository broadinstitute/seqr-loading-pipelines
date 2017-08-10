import argparse
import os
import pip

from utils.elasticsearch_utils import print_elasticsearch_stats

pip.main(['install', 'elasticsearch'])
import elasticsearch

p = argparse.ArgumentParser()
p.add_argument("-H", "--host", default="10.48.0.105")
p.add_argument("-p", "--port", default="30001")

args = p.parse_args()

# to get the ip address, run  `kubectl describe pod elasticsearch-1019229749-vhghc`
ELASTICSEARCH_HOST = args.host #os.environ.get('ELASTICSEARCH_SERVICE_HOST', p.host)
ELASTICSEARCH_PORT = args.port #os.environ.get('ELASTICSEARCH_SERVICE_PORT', p.port) #"9200"
es = elasticsearch.Elasticsearch(ELASTICSEARCH_HOST, port=ELASTICSEARCH_PORT)

print_elasticsearch_stats(es)
