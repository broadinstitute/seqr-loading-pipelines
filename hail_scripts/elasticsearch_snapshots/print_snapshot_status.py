import os
os.system("pip install elasticsearch")

import argparse
import elasticsearch
import json
import requests
import logging
from pprint import pprint

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

p = argparse.ArgumentParser()
p.add_argument("-H", "--host", help="Elasticsearch node host or IP. To look this up, run: `kubectl describe nodes | grep Addresses`", required=True)
p.add_argument("-p", "--port", help="Elasticsearch port", default=9200, type=int)  # 9200
p.add_argument("-r", "--repo", help="optional repository name", default="callsets")
args = p.parse_args()

es = elasticsearch.Elasticsearch(args.host, port=args.port)


# see https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-gcs-repository.html
print("==> getting snapshot status: ")
print(es.snapshot.status(repository=args.repo))

print("==> all snapshots: ")
response = requests.get("http://%s:%s/_snapshot/%s/_all" % (args.host, args.port, args.repo))
pprint(json.loads(response.content))
