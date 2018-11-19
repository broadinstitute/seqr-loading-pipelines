import os
os.system("pip install elasticsearch")

import argparse
import elasticsearch
import json
import logging
from pprint import pprint
import requests

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

p = argparse.ArgumentParser()
p.add_argument("-H", "--host", help="Elasticsearch node host or IP. To look this up, run: `kubectl describe nodes | grep Addresses`", required=True)
p.add_argument("-p", "--port", help="Elasticsearch port", default=9200, type=int)  # 9200
p.add_argument("-b", "--bucket", help="Google bucket name", default="seqr-database-backups")
p.add_argument("-d", "--base-path", help="Path within the bucket", default="elasticsearch/snapshots")
p.add_argument("-r", "--repo", help="Repository name", default="callsets")
p.add_argument("-i", "--index", help="Index name(s). One or more comma-separated index names to include in the snapshot", required=True)
p.add_argument("-w", "--wait-for-completion", action="store_true", help="Whether to wait until the snapshot is created before returning")
args = p.parse_args()

es = elasticsearch.Elasticsearch(args.host, port=args.port)


# see https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-gcs-repository.html
print("==> check if snapshot repo exists: %s" % args.repo)
repo_info = es.snapshot.get_repository(repository=args.repo)
pprint(repo_info)

# see https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html
response = requests.get("http://%s:%s/_snapshot/%s/_all" % (args.host, args.port, args.repo))
all_snapshots = json.loads(response.content).get("snapshots", [])
all_snapshots.sort(key=lambda s: s["start_time_in_millis"])

latest_snapshot = all_snapshots[-1]

snapshot_name = latest_snapshot["snapshot"]

print("==> restoring snapshot: " + snapshot_name)
# http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.client.SnapshotClient.restore
pprint(
    es.snapshot.restore(
        repository=args.repo,
        snapshot=snapshot_name,
        wait_for_completion=args.wait_for_completion,
    )
)

print("==> getting snapshot status for: " + snapshot_name)
pprint(
    es.snapshot.status(repository=args.repo)
)
