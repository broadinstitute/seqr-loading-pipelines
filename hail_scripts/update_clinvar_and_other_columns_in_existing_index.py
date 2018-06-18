import os
os.system("pip install elasticsearch")

import argparse
import elasticsearch
import hail
from pprint import pprint
import logging
import sys
import urllib

from hail_scripts.utils.add_clinvar import add_clinvar_to_vds
from hail_scripts.utils.elasticsearch_client import ElasticsearchClient
from hail_scripts.utils.elasticsearch_utils import ELASTICSEARCH_UPDATE
from hail_scripts.utils.load_vds_utils import read_in_dataset, compute_minimal_schema

logger = logging.getLogger()


def update_clinvar_in_dataset(hc, dataset_vds_path, genome_version, host, port, elasticsearch_index, filter_interval=None, block_size=None):
    elasticsearch_client = ElasticsearchClient(host, port)

    vds = read_in_dataset(hc, dataset_vds_path, filter_interval=filter_interval)
    vds = compute_minimal_schema(vds)

    vds = add_clinvar_to_vds(hc, vds, args.genome_version, root="va.clinvar", subset=filter_interval)

    print("\n\n==> Schema: ")
    pprint(vds.variant_schema)

    logger.info("\n==> Exporting to elasticsearch:")
    elasticsearch_client.export_vds_to_elasticsearch(
        vds,
        index_name=elasticsearch_index,
        index_type_name="variant",
        block_size=block_size,
        elasticsearch_write_operation=ELASTICSEARCH_UPDATE,
        elasticsearch_mapping_id="docId",
        is_split_vds=True,
        verbose=False,
        delete_index_before_exporting=False,
        ignore_elasticsearch_write_errors=False,
    )

    logger.info("\n==> Update operations log")
    elasticsearch_client.save_index_operation_metadata(
        dataset_vds_path,
        elasticsearch_index,
        genome_version,
        #project_id=args.project_guid,
        command=" ".join(sys.argv),
        #directory=args.directory,
        #username=args.username,
        operation="update_clinvar",
        status="success",
    )



        # index_name="data",
    # index_type_name="variant",
    # block_size=5000,
    # num_shards=10,


def update_clinvar_in_all_projects(host, port, block_size=None):
    client = elasticsearch.Elasticsearch(args.host, port=args.port)
    indices = client.cat.indices(h="index").strip().split("\n")


    #update_clinvar_in_dataset(host, port, index, vcf_or_vds_path, clinvar_vds, block_size=block_size)


p = argparse.ArgumentParser()
p.add_argument("--host", help="Elastisearch IP address", default="localhost") # 10.4.0.29
p.add_argument("--port", help="Elastisearch port", default="9200")
p.add_argument("--subset", help="Subset")
p.add_argument("--block-size", help="Block size", type=int, default=1000)
p.add_argument("--genome-version", help="Genome version", choices=["37", "38"], required=True)

#g = p.add_mutually_exclusive_group(required=True)
p.add_argument("--index-name", help="Elasticsearch index name. If specified, only this index will be updated.", required=True)
#g.add_argument("--all", help="Update all elasticsearch indices.", action="store_true")

p.add_argument("dataset_path", help="VDS or VCF path")

# parse args
args = p.parse_args()

hc = hail.HailContext(log="/hail.log")

print("\n==> input: %s" % args.dataset_path)

filter_interval = "1-MT"
if args.subset:
    filter_interval = args.subset

if args.index_name:
    # hc, dataset_vds_path, genome_version, host, port, elasticsearch_index, block_size=None):
    update_clinvar_in_dataset(
        hc,
        args.dataset_path,
        args.genome_version,
        args.host,
        args.port,
        args.index_name,
        filter_interval=filter_interval,
        block_size=args.block_size)


#elif args.all:
#    update_all_indicies(args.host, args.port, block_size=args.block_size)



