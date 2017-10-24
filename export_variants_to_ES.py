#!/usr/bin/env python

import argparse
import hail
import logging
import time
from pprint import pprint
from utils.computed_fields_utils import CONSEQUENCE_TERMS
from utils.elasticsearch_utils import export_vds_to_elasticsearch, DEFAULT_GENOTYPE_FIELDS_TO_EXPORT, \
    DEFAULT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP
from utils.fam_file_utils import MAX_SAMPLES_PER_INDEX, compute_sample_groups_from_fam_file
from utils.gcloud_utils import get_gcloud_file_stats, google_bucket_file_iter

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
p.add_argument("--num-samples", help="Number of samples to include in the output", type=int, default=225)
p.add_argument("--fam-file", help=".fam file used to check VDS sample IDs and assign samples to indices with "
  "a max of 'num_samples' per index, but making sure that samples from the same family don't end up in different indices", required=True)
p.add_argument("--only-coding", action="store_true")
p.add_argument("--only-non-coding", action="store_true")
p.add_argument("--ignore-extra-sample-ids-in-fam-file", action="store_true")
p.add_argument("--ignore-extra-sample-ids-in-vds", action="store_true")
p.add_argument("input_vds", help="input VDS")

# parse args
args = p.parse_args()

input_vds_path = str(args.input_vds)
if not input_vds_path.endswith(".vds"):
    p.error("Input must be a .vds")


logger.info("Input: " + input_vds_path)
logger.info("Output: elasticsearch index @ %(host)s:%(port)s/%(index)s/%(index_type)s" % args.__dict__)

logger.info("==> create HailContext")
hc = hail.HailContext(log="/hail.log")

logger.info("==> import vds: " + input_vds_path)
vds = hc.read(input_vds_path)

logger.info("==> imported dataset")
logger.info(vds.variant_schema)

# compute sample groups
if len(vds.sample_ids) > MAX_SAMPLES_PER_INDEX:
    if not args.fam_file:
        p.exit("--fam-file must be specified for callsets larger than %s samples. This callset has %s samples." % (MAX_SAMPLES_PER_INDEX, len(args.input_vds)))
    else:
        sample_groups = compute_sample_groups_from_fam_file(
            args.fam_file,
            vds.sample_ids,
            args.ignore_extra_sample_ids_in_vds,
            args.ignore_extra_sample_ids_in_fam_file
        )
else:
    sample_groups = [vds.sample_ids]

# filter to coding or non-coding variants
non_coding_consequence_first_index = CONSEQUENCE_TERMS.index("5_prime_UTR_variant")
if args.only_coding:
    logger.info("==> filter to coding variants only (all transcript consequences above 5_prime_UTR_variant)")
    vds = vds.filter_variants_expr("va.mainTranscript.major_consequence_rank < %d" % non_coding_consequence_first_index, keep=True)
elif args.only_non_coding:
    logger.info("==> filter to non-coding variants only (all transcript consequences above 5_prime_UTR_variant)")
    vds = vds.filter_variants_expr("isMissing(va.mainTranscript.major_consequence_rank) || va.mainTranscript.major_consequence_rank >= %d" % non_coding_consequence_first_index, keep=True)


for i, sample_group in enumerate(sample_groups):
    if len(sample_groups) > 1:
        vds_sample_subset = vds.filter_samples_list(sample_group, keep=True)
        index_name = "%s_%s" % (args.index_name, i)
    else:
        vds_sample_subset = vds
        index_name = args.index_name

    logger.info("==> loading %s samples into %s" % (len(sample_group), index_name))
    logger.info("Samples: %s .. %s" % (", ".join(sample_group[:3]), ", ".join(sample_group[-3:])))

    logger.info("==> export to elasticsearch")
    DISABLE_INDEX_FOR_FIELDS = ("sortedTranscriptConsequences", )
    DISABLE_DOC_VALUES_FOR_FIELDS = ("sortedTranscriptConsequences", )

    timestamp1 = time.time()
    export_vds_to_elasticsearch(
        vds_sample_subset,
        genotype_fields_to_export=DEFAULT_GENOTYPE_FIELDS_TO_EXPORT,
        genotype_field_to_elasticsearch_type_map=DEFAULT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP,
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

    timestamp2 = time.time()
    logger.info("==> finished exporting - time: %s seconds" % (timestamp2 - timestamp1))
