import os
import re

os.system("pip install elasticsearch")

import argparse
from pprint import pprint
import logging

from hail_scripts.shared.elasticsearch_utils import ELASTICSEARCH_UPDATE

import hail as hl
from hail_scripts.v02.update_models.update_mt_schema import HGMDSchema, CLINVARSchema, CIDRSchema
from hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient
from lib.model.seqr_mt_schema import SeqrVariantsAndGenotypesSchema

logger = logging.getLogger(__name__)

# Change the paths to your hadoop/gcloud space
clinvar_ht_path='/seqr-reference-data/GRCh37/clinvar/clinvar.GRCh37.ht'
hgmd_ht_path='/seqr-reference-data/GRCh37/hgmd/hgmd_2019.3_hg19_noDB.ht'
cidr_ht_path='/seqr-reference-data/GRCh37/CIDR/Holland_20190829_no_genotypes_selected_cols.ht'


def update_all_datasets(hc, args):
    client = ElasticsearchClient(host=args.host, port=args.port)
    indices = client.es.cat.indices(h="index", s="index").strip().split("\n")
    for i, index_name in enumerate(indices):
        _meta = client.get_index_meta(index_name)

        logger.info("==> updating index {} out of {}: {}".format(i+1, len(indices), index_name))
        if _meta and "sourceFilePath" in _meta:
            logger.info("==> skipping {} because index _meta['sourceFilePath'] isn't set: {}".format(index_name, _meta))
            try:
                update_dataset(index_name, args)
            except Exception as e:
                logger.error("ERROR while updating %s - %s: %s", index_name, _meta["sourceFilePath"], e)
        else:
            logger.info("==> skipping {} because index _meta['sourceFilePath'] isn't set: {}".format(index_name, _meta))


def update_dataset(index_name, args):
    elasticsearch_client = ElasticsearchClient(host=args.host, port=args.port)
    _meta = elasticsearch_client.get_index_meta(index_name)
    if not args.dataset_path and (not _meta or "sourceFilePath" not in _meta):
        raise ValueError("Couldn't update reference data in {} because it doesn't have a recorded sourceFilePath. Please use "
        "--index-name, --dataset-path, and --genome-version to update this index.")

    dataset_path = args.dataset_path or _meta["sourceFilePath"]
    genome_version = args.genome_version or _meta.get("genomeVersion")

    if genome_version is None:
        match = re.search("__grch([0-9]+)__", index_name, re.IGNORECASE)
        if not match:
            raise ValueError("ERROR: couldn't update clinvar in {} because the genome version wasn't found in "
                             "_meta ({}) or in the index name.".format(index_name, _meta))
        genome_version = match.group(1)

    print('dataset_path: %s' % dataset_path)

    # Import the VCFs from inputs. Set min partitions so that local pipeline execution takes advantage of all CPUs.
    mt = hl.import_vcf(dataset_path, reference_genome='GRCh' + genome_version, force_bgz=True, min_partitions=500)

    # When input VCF having a bad property (duplicated loci) we need to run this line
    mt = mt.key_rows_by('locus').distinct_by_row().key_rows_by('locus', 'alleles')

    mt = hl.split_multi_hts(mt.annotate_rows(locus_old=mt.locus, alleles_old=mt.alleles))

    if args.update_clinvar:
        clinvar = hl.read_table(clinvar_ht_path)
        mt = CLINVARSchema(mt, clinvar_data=clinvar).clinvar()

    if args.update_hgmd:
        hgmd = hl.read_table(hgmd_ht_path)
        mt = HGMDSchema(mt, hgmd_data=hgmd).hgmd()

    if args.update_cidr:
        cidr = hl.read_table(cidr_ht_path)
        mt = CIDRSchema(mt, cidr_data=cidr).cidr()

    mt = mt.aIndex().doc_id()
    mt = mt.select_annotated_mt()

    variant_count = mt.count_rows()
    logger.info("\n==> exporting {} variants to elasticsearch:".format(variant_count))

    row_table = mt.rows().flatten()
    row_table = row_table.drop(row_table.locus, row_table.alleles)

    elasticsearch_client.export_table_to_elasticsearch(
        row_table,
        index_name=index_name,
        index_type_name="variant",
        block_size=args.block_size,
        elasticsearch_write_operation=ELASTICSEARCH_UPDATE,
        elasticsearch_mapping_id="docId",
        verbose=False,
        delete_index_before_exporting=False,
        ignore_elasticsearch_write_errors=False,
        export_globals_to_index_meta=True,
    )


p = argparse.ArgumentParser()
p.add_argument("--host", help="Elasticsearch host", default=os.environ.get("ELASTICSEARCH_SERVICE_HOSTNAME"))
p.add_argument("--port", help="Elasticsearch port", default="9200")
p.add_argument("--block-size", help="Block size to use when exporting to elasticsearch", default=1000, type=int)

#p.add_argument("--download-latest-clinvar-vcf", action="store_true", help="First download the latest GRCh37 and GRCh38 clinvar VCFs from NCBI.")
p.add_argument("--update-clinvar", action="store_true", help="Update clinvar fields.")
p.add_argument("--update-hgmd", action="store_true", help="Update hgmd fields.")
p.add_argument("--update-cidr", action="store_true", help="Update cidr fields.")

# these datasets don't get new versions as frequently as ClinVar or HGMD but they do occasionally
#p.add_argument("--update-primate-ai", action="store_true", help="Update PrimateAI fields.")
#p.add_argument("--update-splice-ai", action="store_true", help="Update SpliceAI fields.")

p.add_argument("--index-name", help="Elasticsearch index name. If specified, only this index will be updated.")
p.add_argument("--dataset-path", help="(optional) Path of variant callset. If not specified, the original "
    "vcf/vds path from which the data was loaded will be used.")
p.add_argument("--genome-version", help="Genome build: 37 or 38", choices=["37", "38"])

p.add_argument("--all", help="Update all elasticsearch indices. This option is mutually-exclusive "
    "with --index-name, --dataset-path, and --genome-version.", action="store_true")


args = p.parse_args()

#if args.download_latest_clinvar_vcf:
#    for genome_version in ["37", "38"]:
#        vds = download_and_import_latest_clinvar_vcf(hc, genome_version)
#        write_vds(vds, CLINVAR_VDS_PATH.format(genome_version=genome_version))

if args.index_name and not args.all:
    update_dataset(args.index_name, args)
elif args.all:
    update_all_datasets(args)
else:
    p.exit("ERROR: must specify either --index-name or --all")



