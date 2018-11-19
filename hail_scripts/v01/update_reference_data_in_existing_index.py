import os
import re

from hail_scripts.v01.utils.add_hgmd import add_hgmd_to_vds, reset_hgmd_fields_in_vds
from hail_scripts.v01.utils.hail_utils import create_hail_context

os.system("pip install elasticsearch")

import argparse
from pprint import pprint
import logging

from hail_scripts.shared.elasticsearch_utils import ELASTICSEARCH_UPDATE
from hail_scripts.v01.utils.add_clinvar import add_clinvar_to_vds, download_and_import_latest_clinvar_vcf, CLINVAR_VDS_PATH, \
    reset_clinvar_fields_in_vds
from hail_scripts.v01.utils.elasticsearch_client import ElasticsearchClient
from hail_scripts.v01.utils.vds_utils import read_in_dataset, compute_minimal_schema, write_vds

logger = logging.getLogger()


def update_all_datasets(hc, host, port, **kwargs):
    client = ElasticsearchClient(host, port=port)
    indices = client.es.cat.indices(h="index", s="index").strip().split("\n")
    for index_name in indices:
        _meta = client.get_index_meta(index_name)

        if _meta and "sourceFilePath" in _meta:
            try:
                update_dataset(hc, host, port, index_name, **kwargs)
            except Exception as e:
                logger.error("ERROR while updating %s - %s: %s", index_name, _meta["sourceFilePath"], e)
        else:
            logger.info("Skipping {} because index _meta['sourceFilePath'] isn't set: {}".format(index_name, _meta))


def update_dataset(hc, host, port, index_name, filter_interval=None, block_size=1000, update_clinvar=True, update_hgmd=False):
    if not update_clinvar and not update_hgmd:
        raise ValueError("update_clinvar or update_hgmd must = True")

    elasticsearch_client = ElasticsearchClient(host, port)
    _meta = elasticsearch_client.get_index_meta(index_name)
    if not _meta or "sourceFilePath" not in _meta or not _meta["sourceFilePath"].endswith(".vds"):
        logger.info("ERROR: couldn't update clinvar in {} because sourceFilePath must contain a valid .vds path. _meta['sourceFilePath']: {}".format(index_name, _meta))
        return

    dataset_vds_path = _meta["sourceFilePath"]
    genome_version = _meta.get("genomeVersion")

    if genome_version is None:
        match = re.search("__grch([0-9]+)__", index_name, re.IGNORECASE)
        if not match:
            logger.info("ERROR: couldn't update clinvar in {} because the genome version wasn't found in _meta ({}) or in the index name.".format(index_name, _meta))
            return
        genome_version = match.group(1)

    vds = read_in_dataset(hc, dataset_vds_path, filter_interval=filter_interval)
    vds = vds.drop_samples()
    vds = compute_minimal_schema(vds)

    if update_clinvar:
        #vds = reset_clinvar_fields_in_vds(hc, vds, genome_version, root="va.clinvar", subset=filter_interval)
        vds = add_clinvar_to_vds(hc, vds, genome_version, root="va.clinvar", subset=filter_interval)

    if update_hgmd:
        #vds = reset_hgmd_fields_in_vds(hc, vds, genome_version, root="va.hgmd", subset=filter_interval)
        vds = add_hgmd_to_vds(hc, vds, genome_version, root="va.hgmd", subset=filter_interval)

    vds = vds.annotate_global_expr('global.genomeVersion = "{}"'.format(genome_version))

    print("\n\n==> schema: ")
    pprint(vds.variant_schema)

    logger.info("\n==> exporting to elasticsearch:")
    elasticsearch_client.export_vds_to_elasticsearch(
        vds,
        index_name=index_name,
        index_type_name="variant",
        block_size=block_size,
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
p.add_argument("--download-latest-clinvar-vcf", action="store_true",
               help="First download the latest GRCh37 and GRCh38 clinvar VCFs from NCBI.")
p.add_argument("--update-clinvar", action="store_true", help="Update clinvar fields.")
p.add_argument("--update-hgmd", action="store_true", help="Update hgmd fields.")

g = p.add_mutually_exclusive_group(required=True)
g.add_argument("--index-name", help="Elasticsearch index name. If specified, only this index will be updated.")
g.add_argument("--all", help="Update all elasticsearch indices.", action="store_true")


args = p.parse_args()

hc = create_hail_context()

filter_interval = "1-MT"

if args.download_latest_clinvar_vcf:
    for genome_version in ["37", "38"]:
        vds = download_and_import_latest_clinvar_vcf(hc, genome_version)
        write_vds(vds, CLINVAR_VDS_PATH.format(genome_version=genome_version))

kwargs = {
    'filter_interval': filter_interval,
    'update_clinvar': args.update_clinvar,
    'update_hgmd': args.update_hgmd,
}

if args.index_name:
    update_dataset(hc, args.host, args.port, args.index_name, **kwargs)
elif args.all:
    update_all_datasets(hc, args.host, args.port, **kwargs)



