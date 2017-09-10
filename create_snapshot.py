import pip

pip.main(['install', 'elasticsearch'])

import argparse
import elasticsearch
import logging
from pprint import pprint
import time

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

p = argparse.ArgumentParser()
p.add_argument("-H", "--host", help="Elasticsearch node host or IP. To look this up, run: `kubectl describe nodes | grep Addresses`", required=True)
p.add_argument("-p", "--port", help="Elasticsearch port", default=30001, type=int)  # 9200
p.add_argument("-b", "--bucket", help="Google bucket name", default="seqr-database-backups")

p.add_argument("-r", "--repo", help="Repository name", default="elasticsearch-prod")


# parse args
args = p.parse_args()

es = elasticsearch.Elasticsearch(args.host, port=args.port)

# see http://elasticsearch-py.readthedocs.io/en/master/api.html#snapshot
body = {
    "type": "gcs",
    "settings": {
        "bucket": args.bucket,
        "compress": True,
    }
}

# see https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-gcs-repository.html
es.snapshot.create_repository(repository=args.repo, body=body)

# see https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html
snapshot_name = "snapshot_"+time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime())

#es.snapshot.create(repository=args.repo, snapshot=snapshot_name, body={
    #"indices": "index_1,index_2"
#})

print("Getting snapshot status for: " + snapshot_name)
print(es.snapshot.status(repository=args.repo))
