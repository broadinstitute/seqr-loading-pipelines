import os
import re

from hail_scripts.v01.utils.add_primate_ai import add_primate_ai_to_vds
from hail_scripts.v01.utils.add_splice_ai import add_splice_ai_to_vds

os.system("pip install elasticsearch")

import argparse
from pprint import pprint
import logging

from hail_scripts.shared.elasticsearch_utils import ELASTICSEARCH_UPDATE
from hail_scripts.v01.utils.add_clinvar import add_clinvar_to_vds, download_and_import_latest_clinvar_vcf, CLINVAR_VDS_PATH
from hail_scripts.v01.utils.add_hgmd import add_hgmd_to_vds
from hail_scripts.v01.utils.hail_utils import create_hail_context
from hail_scripts.v01.utils.elasticsearch_client import ElasticsearchClient
from hail_scripts.v01.utils.vds_utils import read_in_dataset, compute_minimal_schema, write_vds

logger = logging.getLogger()


def update_all_datasets(hc, args):
    client = ElasticsearchClient(args.host, port=args.port)
    indices = client.es.cat.indices(h="index", s="index").strip().split("\n")
    for i, index_name in enumerate(indices):
        _meta = client.get_index_meta(index_name)

        logger.info("==> updating index {} out of {}: {}".format(i+1, len(indices), index_name))
        if _meta and "sourceFilePath" in _meta:
            logger.info("==> skipping {} because index _meta['sourceFilePath'] isn't set: {}".format(index_name, _meta))
            try:
                update_dataset(hc, index_name, args)
            except Exception as e:
                logger.error("ERROR while updating %s - %s: %s", index_name, _meta["sourceFilePath"], e)
        else:
            logger.info("==> skipping {} because index _meta['sourceFilePath'] isn't set: {}".format(index_name, _meta))


def update_dataset(hc, index_name, args):

    elasticsearch_client = ElasticsearchClient(args.host, args.port)
    _meta = elasticsearch_client.get_index_meta(index_name)
    if not args.dataset_path and (not _meta or "sourceFilePath" not in _meta):
        logger.error("Couldn't update reference data in {} because it doesn't have a recorded sourceFilePath. Please use "
        "--index-name, --dataset-path, and --genome-version to update this index.".format(index_name))
        return

    dataset_path = args.dataset_path or _meta["sourceFilePath"]
    genome_version = args.genome_version or _meta.get("genomeVersion")

    if genome_version is None:
        match = re.search("__grch([0-9]+)__", index_name, re.IGNORECASE)
        if not match:
            logger.info("ERROR: couldn't update clinvar in {} because the genome version wasn't found in _meta ({}) or in the index name.".format(index_name, _meta))
            return
        genome_version = match.group(1)

    vds = read_in_dataset(hc, dataset_path)
    vds = vds.drop_samples()
    vds = compute_minimal_schema(vds)
    vds = vds.annotate_global_expr('global.genomeVersion = "{}"'.format(genome_version))

    # add reference data to vds
    filter_expr = []
    if args.update_primate_ai:
        vds = add_primate_ai_to_vds(hc, vds, genome_version, root="va.primate_ai")
        filter_expr.append("isDefined(va.primate_ai.score)")

    if args.update_splice_ai:
        vds = add_splice_ai_to_vds(hc, vds, genome_version, root="va.splice_ai")
        filter_expr.append("isDefined(va.splice_ai.delta_score)")

    if args.update_clinvar:
        #vds = reset_clinvar_fields_in_vds(hc, vds, genome_version, root="va.clinvar", subset=filter_interval)
        vds = add_clinvar_to_vds(hc, vds, genome_version, root="va.clinvar")
        filter_expr.append("isDefined(va.clinvar.allele_id)")

    if args.update_hgmd:
        #vds = reset_hgmd_fields_in_vds(hc, vds, genome_version, root="va.hgmd", subset=filter_interval)
        vds = add_hgmd_to_vds(hc, vds, genome_version, root="va.hgmd")
        filter_expr.append("isDefined(va.hgmd.accession)")

    # filter down to variants that have reference data

    vds = vds.filter_variants_expr(" || ".join(filter_expr), keep=True)

    print("\n\n==> schema: ")
    pprint(vds.variant_schema)

    _, variant_count = vds.count()
    logger.info("\n==> exporting {} variants to elasticsearch:".format(variant_count))
    elasticsearch_client.export_vds_to_elasticsearch(
        vds,
        index_name=index_name,
        index_type_name="variant",
        block_size=args.block_size,
        elasticsearch_write_operation=ELASTICSEARCH_UPDATE,
        elasticsearch_mapping_id="docId",
        is_split_vds=True,
        verbose=False,
        delete_index_before_exporting=False,
        ignore_elasticsearch_write_errors=False,
        export_globals_to_index_meta=True,
    )


p = argparse.ArgumentParser()
p.add_argument("--host", help="Elasticsearch host", default=os.environ.get("ELASTICSEARCH_SERVICE_HOSTNAME"))
p.add_argument("--port", help="Elasticsearch port", default="9200")
p.add_argument("--block-size", help="Block size to use when exporting to elasticsearch", default=1000, type=int)

p.add_argument("--download-latest-clinvar-vcf", action="store_true", help="First download the latest GRCh37 and GRCh38 clinvar VCFs from NCBI.")
p.add_argument("--update-clinvar", action="store_true", help="Update clinvar fields.")
p.add_argument("--update-hgmd", action="store_true", help="Update hgmd fields.")

# these datasets don't get new versions as frequently as ClinVar or HGMD but they do occasionally
p.add_argument("--update-primate-ai", action="store_true", help="Update PrimateAI fields.")
p.add_argument("--update-splice-ai", action="store_true", help="Update SpliceAI fields.")

p.add_argument("--index-name", help="Elasticsearch index name. If specified, only this index will be updated.")
p.add_argument("--dataset-path", help="(optional) Path of variant callset. If not specified, the original "
    "vcf/vds path from which the data was loaded will be used.")
p.add_argument("--genome-version", help="Genome build: 37 or 38", choices=["37", "38"])

p.add_argument("--all", help="Update all elasticsearch indices. This option is mutually-exclusive "
    "with --index-name, --dataset-path, and --genome-version.", action="store_true")


args = p.parse_args()

hc = create_hail_context()

if args.download_latest_clinvar_vcf:
    for genome_version in ["37", "38"]:
        vds = download_and_import_latest_clinvar_vcf(hc, genome_version)
        write_vds(vds, CLINVAR_VDS_PATH.format(genome_version=genome_version))

if args.index_name and not args.all:
    update_dataset(hc, args.index_name, args)
elif args.all:
    update_all_datasets(hc, args)
else:
    p.exit("ERROR: must specify either --index-name or --all")



