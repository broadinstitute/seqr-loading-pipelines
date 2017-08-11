#!/usr/bin/env python

import argparse
import hail
from pprint import pprint
from utils.elasticsearch_utils import export_vds_to_elasticsearch

p = argparse.ArgumentParser()
p.add_argument("-H", "--host", help="Elasticsearch node host or IP. To look this up, run: `kubectl describe nodes | grep Addresses`", required=True)
p.add_argument("-p", "--port", help="Elasticsearch port", default=30001, type=int)
p.add_argument("-i", "--index", help="Elasticsearch index name", default="variant_callset")
p.add_argument("-t", "--index-type", help="Elasticsearch index type", default="variant")
p.add_argument("-b", "--block-size", help="Elasticsearch block size", default=1000)
p.add_argument("input_vds", help="input VDS")

# parse args
args = p.parse_args()

input_vds_path = str(args.input_vds)
if not input_vds_path.endswith(".vds"):
    p.error("Input must be a .vds")

print("Input: " + input_vds_path)
print("Output: elasticsearch index @ %(host)s:%(port)s/%(index)s/%(index_type)s" % args.__dict__)

print("\n==> create HailContext")
hc = hail.HailContext(log="/hail.log")

print("\n==> import vds: " + input_vds_path)
vds = hc.read(input_vds_path)

print("\n==> imported dataset")
pprint(vds.variant_schema)

print("\n==> exporting to ES")
MAX_SAMPLES_PER_INDEX = 100
NUM_INDEXES = 1 + (len(vds.sample_ids) - 1)/MAX_SAMPLES_PER_INDEX
for i in range(NUM_INDEXES):
    index_name = "%s_%s" % (args.index, i)
    print("\n==> load samples %s to %s of %s samples into %s" % (i*MAX_SAMPLES_PER_INDEX, (i+1)*MAX_SAMPLES_PER_INDEX, len(vds.sample_ids), index_name))

    vds_sample_subset = vds.filter_samples_list(vds.sample_ids[i*MAX_SAMPLES_PER_INDEX:(i+1)*MAX_SAMPLES_PER_INDEX], keep=True)
    print("\n==> export to elasticsearch")
    DISABLE_INDEX_FOR_FIELDS = ("sortedTranscriptConsequences", )
    DISABLE_DOC_VALUES_FOR_FIELDS = ("sortedTranscriptConsequences", )

    export_vds_to_elasticsearch(
        vds_sample_subset,
        export_genotypes=True,
        host=args.host,
        port=args.port,
        index_name=index_name,
        index_type_name=args.index_type,
        block_size=args.block_size,
        delete_index_before_exporting=True,
        disable_doc_values_for_fields=DISABLE_DOC_VALUES_FOR_FIELDS,
        disable_index_for_fields=DISABLE_INDEX_FOR_FIELDS,
        is_split_vds=True,
        verbose=True,
    )
