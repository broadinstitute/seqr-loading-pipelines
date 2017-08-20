#!/usr/bin/env python

import argparse
import hail
import logging
from pprint import pprint

from utils.computed_fields_utils import CONSEQUENCE_TERMS
from utils.elasticsearch_utils import export_vds_to_elasticsearch
import sys

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

p = argparse.ArgumentParser()
p.add_argument("-H", "--host", help="Elasticsearch node host or IP. To look this up, run: `kubectl describe nodes | grep Addresses`", required=True)
p.add_argument("-p", "--port", help="Elasticsearch port", default=30001, type=int)
p.add_argument("-i", "--index", help="Elasticsearch index name", default="variant_callset")
p.add_argument("-t", "--index-type", help="Elasticsearch index type", default="variant")
p.add_argument("-b", "--block-size", help="Elasticsearch block size", default=1000, type=int)
p.add_argument("-s", "--num-shards", help="Number of shards to use for this index (see https://www.elastic.co/guide/en/elasticsearch/guide/current/overallocation.html)", default=8, type=int)
p.add_argument("--num-samples", help="Number of samples to include in the output", type=int)
p.add_argument("--only-coding", action="store_true")
p.add_argument("--only-non-coding", action="store_true")
p.add_argument("input_vds", help="input VDS")

# parse args
args = p.parse_args()

input_vds_path = str(args.input_vds)
if not input_vds_path.endswith(".vds"):
    p.error("Input must be a .vds")

logger.info("Input: " + input_vds_path)
logger.info("Output: elasticsearch index @ %(host)s:%(port)s/%(index)s/%(index_type)s" % args.__dict__)

logger.info("\n==> create HailContext")
hc = hail.HailContext(log="/hail.log")

logger.info("\n==> import vds: " + input_vds_path)
vds = hc.read(input_vds_path)

logger.info("\n==> imported dataset")
logger.info(vds.variant_schema)

# filter to coding or non-coding variants
non_coding_consequence_first_index = CONSEQUENCE_TERMS.index("5_prime_UTR_variant")
if args.only_coding:
    logger.info("\n==> filter to coding variants only (all transcript consequences above 5_prime_UTR_variant)")
    vds = vds.filter_variants_expr("va.mainTranscript.major_consequence_rank < %d" % non_coding_consequence_first_index, keep=True)
elif args.only_non_coding:
    logger.info("\n==> filter to non-coding variants only (all transcript consequences above 5_prime_UTR_variant)")
    vds = vds.filter_variants_expr("isMissing(va.mainTranscript.major_consequence_rank) || va.mainTranscript.major_consequence_rank >= %d" % non_coding_consequence_first_index, keep=True)


logger.info("\n==> exporting to ES")
#MAX_SAMPLES_PER_INDEX = 100
#NUM_INDEXES = 1 + (len(vds.sample_ids) - 1)/MAX_SAMPLES_PER_INDEX

if not args.num_samples:
    sample_groups = [
        #   samples[0:100],
        #   samples[100:200],
        #   samples[200:300],
        #   samples[300:400],
        #   samples[400:501],
        #   samples[501:602],
        #   samples[602:701],
        #   samples[701:802],
        #   samples[802:905],

        #    vds.sample_ids[0:300],
        #    vds.sample_ids[300:602],   # split on family boundaries
        #    vds.sample_ids[602:900],

        vds.sample_ids,
    ]

else:
    sample_groups = [
        vds.sample_ids[:args.num_samples],
    ]




for i, sample_group in enumerate(sample_groups):

    index_name = "%s_%s" % (args.index, i)
    logger.info("\n==> loading %s samples into %s" % (len(sample_group), index_name))

    vds_sample_subset = vds.filter_samples_list(sample_group, keep=True)

    logger.info("\n==> export to elasticsearch")
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
        num_shards=args.num_shards,
        delete_index_before_exporting=True,
        disable_doc_values_for_fields=DISABLE_DOC_VALUES_FOR_FIELDS,
        disable_index_for_fields=DISABLE_INDEX_FOR_FIELDS,
        is_split_vds=True,
        verbose=True,
    )
