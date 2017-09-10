#!/usr/bin/env python

import argparse
import hail
import logging
from pprint import pprint
import time
from utils.computed_fields_utils import CONSEQUENCE_TERMS
from utils.elasticsearch_utils import export_vds_to_elasticsearch
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

if get_gcloud_file_stats(args.fam_file) is None:
    p.error("%s not found" % args.fam_file)

family_ids = set()
individual_id_to_family_id = {}
for line_counter, line in enumerate(google_bucket_file_iter(args.fam_file)):
    if line.startswith("#") or line.strip() == "":
        continue
    fields = line.split("\t")
    if len(fields) < 6:
        raise ValueError("Unexpected .fam file format on line %s: %s" % (line_counter+1, line))

    family_id = fields[0]
    individual_id = fields[1]

    family_ids.add(family_id)
    individual_id_to_family_id[individual_id] = family_id

logger.info("Parsed %s families and %s individuals from %s" % (len(family_ids), line_counter + 1, args.fam_file))

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

# filter to coding or non-coding variants
non_coding_consequence_first_index = CONSEQUENCE_TERMS.index("5_prime_UTR_variant")
if args.only_coding:
    logger.info("==> filter to coding variants only (all transcript consequences above 5_prime_UTR_variant)")
    vds = vds.filter_variants_expr("va.mainTranscript.major_consequence_rank < %d" % non_coding_consequence_first_index, keep=True)
elif args.only_non_coding:
    logger.info("==> filter to non-coding variants only (all transcript consequences above 5_prime_UTR_variant)")
    vds = vds.filter_variants_expr("isMissing(va.mainTranscript.major_consequence_rank) || va.mainTranscript.major_consequence_rank >= %d" % non_coding_consequence_first_index, keep=True)

sample_ids_in_vds_and_not_in_fam_file = []
sample_ids_in_fam_file_and_not_in_vds = []
for sample_id in vds.sample_ids:
    if sample_id not in individual_id_to_family_id:
        sample_ids_in_vds_and_not_in_fam_file.append(sample_id)

for sample_id in individual_id_to_family_id:
    if sample_id not in vds.sample_ids:
        sample_ids_in_fam_file_and_not_in_vds.append(sample_id)


if sample_ids_in_vds_and_not_in_fam_file and not args.ignore_extra_sample_ids_in_vds:
    p.error("%s sample ids from vds not found in .fam file (%s)" % (len(sample_ids_in_vds_and_not_in_fam_file), ", ".join(sample_ids_in_vds_and_not_in_fam_file)))

if sample_ids_in_fam_file_and_not_in_vds and not args.ignore_extra_sample_ids_in_fam_file:
    p.error("%s sample ids from .fam file not found in vds (%s)" % (len(sample_ids_in_fam_file_and_not_in_vds), ", ".join(sample_ids_in_fam_file_and_not_in_vds)))


sample_groups = []
previous_family_id = None
current_sample_group = []
for sample_id in vds.sample_ids:
    family_id = individual_id_to_family_id[sample_id]
    if len(current_sample_group) >= args.num_samples and family_id != previous_family_id:
        sample_groups.append(current_sample_group)
        current_sample_group = []
    current_sample_group.append(sample_id)
    previous_family_id = family_id

if current_sample_group:
    sample_groups.append(current_sample_group)

logger.info("sample groups: ")
for i, sample_group in enumerate(sample_groups):
    logger.info("%s individuals in sample group %s: %s" % (
        len(sample_group), i, ", ".join(["%s:%s" % (individual_id_to_family_id[sample_id], sample_id) for sample_id in sample_group])))

#MAX_SAMPLES_PER_INDEX = 100
#NUM_INDEXES = 1 + (len(vds.sample_ids) - 1)/MAX_SAMPLES_PER_INDEX
#if not args.num_samples:
    #sample_groups = [
        #   samples[0:100],
        #   samples[100:200],
        #   samples[200:300],
        #   samples[300:400],
        #   samples[400:501],
        #   samples[501:602],
        #   samples[602:701],
        #   samples[701:802],
        #   samples[802:905],


        # 5 indexes
        #vds.sample_ids[0:200],
        #vds.sample_ids[200:400],
        #vds.sample_ids[400:602],
        #vds.sample_ids[602:802],
        #vds.sample_ids[802:905],

        # 4 indexes
        #vds.sample_ids[0:225],
        #vds.sample_ids[225:449],
        #vds.sample_ids[449:674],
        #vds.sample_ids[674:900],
        #vds.sample_ids[900:1125],

        # 3 indexes
        #    vds.sample_ids[0:300],
        #    vds.sample_ids[300:602],   # split on family boundaries
        #    vds.sample_ids[602:900],

        # 1 index
        #vds.sample_ids,
    #]


    #sample_groups = [
    #    vds.sample_ids[:args.num_samples],
    #]


for i, sample_group in enumerate(sample_groups):

    index_name = "%s_%s" % (args.index, i)
    logger.info("==> loading %s samples into %s" % (len(sample_group), index_name))
    logger.info("Samples: %s .. %s" % (", ".join(sample_group[:3]), ", ".join(sample_group[-3:])))
    vds_sample_subset = vds.filter_samples_list(sample_group, keep=True)

    logger.info("==> export to elasticsearch")
    DISABLE_INDEX_FOR_FIELDS = ("sortedTranscriptConsequences", )
    DISABLE_DOC_VALUES_FOR_FIELDS = ("sortedTranscriptConsequences", )

    timestamp1 = time.time()
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

    timestamp2 = time.time()
    logger.info("==> finished exporting - time: %s seconds" % (timestamp2 - timestamp1))
