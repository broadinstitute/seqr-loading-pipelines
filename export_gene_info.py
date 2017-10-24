#!/usr/bin/env python

import argparse
import hail
from pprint import pprint
from utils.elasticsearch_utils import export_kt_to_elasticsearch

p = argparse.ArgumentParser()
p.add_argument("-H", "--host", help="Elasticsearch node host or IP. To look this up, run: `kubectl describe nodes | grep Addresses`", required=True)
p.add_argument("-p", "--port", help="Elasticsearch port", default=9200, type=int)
p.add_argument("-i", "--index", help="Elasticsearch index name", default="genes")
p.add_argument("-t", "--index-type", help="Elasticsearch index type", default="genes")
p.add_argument("-b", "--block-size", help="Elasticsearch block size", default=1000, type=int)
p.add_argument("-s", "--num-shards", help="Number of shards", default=2, type=int)

# parse args
args = p.parse_args()

hc = hail.HailContext(log="/hail.log")

kt_coverage = hc.import_table(COVERAGE_PATHS)
kt_coverage = kt_coverage.rename({
    '#chrom': 'chrom',
    '1': 'over1',
    '5': 'over5',
    '10': 'over10',
    '15': 'over15',
    '20': 'over20',
    '25': 'over25',
    '30': 'over30',
    '50': 'over50',
    '100': 'over100',
})
print(kt_coverage.schema)

print("======== Export to elasticsearch ======")
export_kt_to_elasticsearch(
    kt_coverage,
    host=args.host,
    port=args.port,
    index_name=args.index,
    index_type_name=args.index_type,
    num_shards=args.num_shards,
    block_size=args.block_size,
    delete_index_before_exporting=True,
    verbose=True
)
