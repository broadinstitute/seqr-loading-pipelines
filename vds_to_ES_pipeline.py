#!/usr/bin/env python

import argparse
import logging
import os
import time

from utils.gcloud_utils import get_gcloud_file_stats

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

p = argparse.ArgumentParser()
p.add_argument("--cluster", help="dataproc cluster name", default="no-vep")
p.add_argument("--project", help="gcloud project name", default="seqr-project")
p.add_argument("--fam-file", help=".fam file used to check VDS sample IDs and assign samples to indices with "
    "a max of 'num_samples' per index, but making sure that samples from the same family don't end up in different indices", required=True)
p.add_argument("-g", "--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], required=True)
p.add_argument("-H", "--host", help="Elasticsearch node host or IP. To look this up, run: `kubectl describe nodes | grep Addresses`", required=True)
p.add_argument("-p", "--port", help="Elasticsearch port", default=9200, type=int)
p.add_argument("-i", "--index", help="Elasticsearch index name", default="variant_callset")
p.add_argument("-t", "--index-type", help="Elasticsearch index type", default="variant")
p.add_argument("-b", "--block-size", help="Elasticsearch block size", default=1000, type=int)
p.add_argument("-s", "--num-shards", help="Number of shards to use for this index (see https://www.elastic.co/guide/en/elasticsearch/guide/current/overallocation.html)", default=8, type=int)
p.add_argument("--num-samples", help="Number of samples to include in the output", type=int)

p.add_argument("--only-coding", action="store_true")
p.add_argument("--only-non-coding", action="store_true")

p.add_argument("--skip-vep", action="store_true")
p.add_argument("--skip-annotation", action="store_true")
p.add_argument("--skip-export", action="store_true")

p.add_argument("--dry-run", action="store_true")

p.add_argument("input_file", help="input VDS or vcf")


# parse args
args = p.parse_args()

input_file = str(args.input_file)
output_file_prefix = input_file.replace(".vcf", "").replace(".vds", "").replace(".bgz", "").replace(".gz", "")

logger.info("Input: " + input_file)
logger.info("Output: elasticsearch index @ %(host)s:%(port)s/%(index)s/%(index_type)s" % args.__dict__)

cluster = args.cluster
project = args.project
fam_file = args.fam_file

genome_version = args.genome_version
host = args.host
port = args.port
index_name = args.index
block_size = args.block_size
num_shards = args.num_shards
num_samples = args.num_samples

skip_vep = args.skip_vep
skip_annotation = args.skip_annotation
skip_export = args.skip_export


def run(label, command):
    timestamp1 = time.time()
    logger.info("==> " + command)
    if not args.dry_run:
        os.system(command)
        timestamp2 = time.time()
        logger.info("==> finished running %s - time: %s seconds" % (label, timestamp2 - timestamp1))

output_file = None


if not skip_vep:
    logger.info("===============")
    logger.info("    RUN VEP    ")
    logger.info("===============")
    output_file = output_file_prefix + ".vep.vds"

    logger.info("Input file: " + input_file)
    logger.info("Output file: " + output_file)

    if not args.dry_run and get_gcloud_file_stats(input_file) is None:
        p.error("Input file doesn't exist: %s" % input_file)

    run("vep",
        " ".join(["./submit.py --cluster %(cluster)s --project %(project)s",
        "run_vep.py %(input_file)s %(output_file)s"]) % locals())

    input_file = output_file

if not skip_annotation:
    logger.info("======================")
    logger.info("    RUN ANNOTATION    ")
    logger.info("======================")

    output_file = input_file.replace(".vds", "")
    only_coding_arg = ""
    if args.only_coding:
        output_file += ".coding"
        only_coding_arg = "--only-coding"

    only_non_coding_arg = ""
    if args.only_non_coding:
        output_file += ".non_coding"
        only_non_coding_arg = "--only-non-coding"

    output_file += ".all_annotations.vds"

    logger.info("Input file: " + input_file)
    logger.info("Output file: " + output_file)

    if not args.dry_run and get_gcloud_file_stats(input_file) is None:
        p.error("Input file doesn't exist: %s" % input_file)

    run("add annotations",
        " ".join([
            "./submit.py --cluster %(cluster)s --project %(project)s",
            "add_annotations.py",
            "%(only_coding_arg)s %(only_non_coding_arg)s",
            "-g %(genome_version)s",
            "--exclude-cadd --exclude-gnomad",
            "%(input_file)s",
            "%(output_file)s",
        ]) % locals())

    input_file = output_file

if not skip_export:
    logger.info("======================")
    logger.info("    EXPORT TO ES      ")
    logger.info("======================")

    logger.info("Input file: " + input_file)
    logger.info("Output file: " + output_file)

    if not args.dry_run and get_gcloud_file_stats(input_file) is None:
        p.error("Input file doesn't exist: %s" % input_file)

    run("export",
        " ".join([
            "./submit.py --cluster %(cluster)s --project %(project)s",
            "export_variants_to_ES.py -H %(host)s --port %(port)s --fam-file %(fam_file)s --index %(index_name)s %(input_file)s"]) % locals())
