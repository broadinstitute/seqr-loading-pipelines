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
p.add_argument("-b", "--bucket", help="Google bucket name", default="seqr-database-backups")
p.add_argument("-d", "--base-path", help="Path within the bucket", default="elasticsearch/snapshots")
p.add_argument("-r", "--repo", help="Repository name", default="callsets")
args = p.parse_args()

es = elasticsearch.Elasticsearch(args.host, port=args.port)

# see http://elasticsearch-py.readthedocs.io/en/master/api.html#snapshot
body = {
    "type": "gcs",
    "settings": {
        "bucket": args.bucket,
        "base_path": args.base_path,
        "client": "default",   # from https://discuss.elastic.co/t/error-when-loading-google-cloud-storage-credentials-file/94370/9
        "compress": True,
    }
}

print("==> creating repository %s" % (args.repo, ))
pprint(body)

pprint(
    es.snapshot.create_repository(repository=args.repo, body=body)
)