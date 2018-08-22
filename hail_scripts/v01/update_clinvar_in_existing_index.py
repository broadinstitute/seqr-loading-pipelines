import os
import re

from hail_scripts.v01.utils.hail_utils import create_hail_context

os.system("pip install elasticsearch")

import argparse
import hail
from pprint import pprint
import logging

from hail_scripts.shared.elasticsearch_utils import ELASTICSEARCH_UPDATE
from hail_scripts.v01.utils.add_clinvar import add_clinvar_to_vds, download_and_import_latest_clinvar_vcf, CLINVAR_VDS_PATH, \
    reset_clinvar_fields_in_vds
from hail_scripts.v01.utils.elasticsearch_client import ElasticsearchClient
from hail_scripts.v01.utils.vds_utils import read_in_dataset, compute_minimal_schema, write_vds

logger = logging.getLogger()


def update_clinvar_in_all_datasets(host, port):
    client = ElasticsearchClient(host, port=port)
    indices = client.es.cat.indices(h="index").strip().split("\n")
    for index_name in indices:
        _meta = client.get_index_meta(index_name)

        if _meta and "sourceFilePath" in _meta:
            update_clinvar_in_dataset(hc, host, port, index_name)
        else:
            logger.info("Skipping {} because index _meta['sourceFilePath'] isn't set".format(index_name))


def update_clinvar_in_dataset(hc, host, port, index_name, filter_interval=None, block_size=1000):
    elasticsearch_client = ElasticsearchClient(host, port)
    _meta = elasticsearch_client.get_index_meta(index_name)
    if not _meta or "sourceFilePath" not in _meta or not _meta["sourceFilePath"].endswith(".vds"):
        logger.info("ERROR: couldn't update clinvar in {} because sourceFilePath must contain a valid .vds path. _meta['sourceFilePath'] = {}".format(index_name, _meta))
        return

    dataset_vds_path = _meta["sourceFilePath"]

    match = re.search("__grch([0-9]+)__", index_name, re.IGNORECASE)
    if not match:
        logger.info("ERROR: couldn't update clinvar in {} because genome_version string not found in index name.".format(index_name))
        return
    genome_version = match.group(1)

    vds = read_in_dataset(hc, dataset_vds_path, filter_interval=filter_interval)
    vds = vds.drop_samples()
    vds = compute_minimal_schema(vds)

    vds = reset_clinvar_fields_in_vds(hc, vds, genome_version, root="va.clinvar", subset=filter_interval)
    vds = add_clinvar_to_vds(hc, vds, genome_version, root="va.clinvar", subset=filter_interval)

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
p.add_argument("--host", help="Elastisearch IP address", required=True)
p.add_argument("--port", help="Elastisearch port", default="9200")
p.add_argument("--skip-downloading-clinvar-vcf", action="store_true",
               help="Skip the 1st step of downloading and updating the GRCh37 and GRCh38 clinvar VDS.")

g = p.add_mutually_exclusive_group(required=True)
g.add_argument("--index-name", help="Elasticsearch index name. If specified, only this index will be updated.")
g.add_argument("--all", help="Update all elasticsearch indices.", action="store_true")

args = p.parse_args()

hc = create_hail_context()

filter_interval = "1-MT"

if not args.skip_downloading_clinvar_vcf:
    for genome_version in ["37", "38"]:
        vds = download_and_import_latest_clinvar_vcf(hc, genome_version)
        write_vds(vds, CLINVAR_VDS_PATH.format(genome_version=genome_version))


if args.index_name:
    update_clinvar_in_dataset(
        hc,
        args.host,
        args.port,
        args.index_name,
        filter_interval=filter_interval)
elif args.all:
    update_clinvar_in_all_datasets(args.host, args.port)



