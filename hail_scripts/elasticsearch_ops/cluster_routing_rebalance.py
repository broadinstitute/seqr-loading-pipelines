import os
os.system("pip install elasticsearch")

import argparse
import elasticsearch
import logging

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

p = argparse.ArgumentParser()
p.add_argument("-H", "--host", help="Elasticsearch host or IP. To look this up, run: `kubectl describe nodes | grep Addresses`", required=True)
p.add_argument("-p", "--port", help="Elasticsearch port", default=9200, type=int)  # 9200
g = p.add_mutually_exclusive_group(required=True)
g.add_argument("--enable", dest="enable_rebalance", action="store_true", help="Enable auto-rebalancing")
g.add_argument("--disable", dest="enable_rebalance", action="store_false", help="Disable auto-rebalancing")
args = p.parse_args()


value = "all" if args.enable_rebalance else "none"
logger.info("==> %s rebalance: setting cluster.routing.rebalance.enable = %s", "enable" if args.enable_rebalance else "disable", value)

es = elasticsearch.Elasticsearch(args.host, port=args.port)
es.cluster.put_settings({
    "transient" : {
        "cluster.routing.rebalance.enable" : value,
    }
})
