import os
os.system("pip install elasticsearch")

import argparse
import elasticsearch
import logging
from pprint import pprint

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

p = argparse.ArgumentParser()
p.add_argument("-H", "--host", help="Elasticsearch node host or IP. To look this up, run: `kubectl describe nodes | grep Addresses`", required=True)
p.add_argument("-p", "--port", help="Elasticsearch port", default=9200, type=int)  # 9200
args = p.parse_args()

es = elasticsearch.Elasticsearch(args.host, port=args.port)

print("==> get repositories")
pprint(es.snapshot.get_repository())
