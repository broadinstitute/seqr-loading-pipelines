#!/usr/bin/env python

import os

# os.system("pip install elasticsearch")

import argparse
import hail as hl
import logging
from pprint import pprint, pformat
import time
import sys

from hail_scripts.shared.elasticsearch_utils import ELASTICSEARCH_INDEX, \
    ELASTICSEARCH_UPDATE, ELASTICSEARCH_UPSERT
from hail_scripts.v01.utils.add_hgmd import add_hgmd_to_vds
from hail_scripts.v01.utils.add_eigen import add_eigen_to_vds
from hail_scripts.v01.utils.gcloud_utils import delete_gcloud_file
from hail_scripts.v02.utils.vds_utils import read_in_dataset
from hail_scripts.v01.utils.computed_fields import get_expr_for_variant_id, \
    get_expr_for_vep_gene_ids_set, get_expr_for_vep_transcript_ids_set, \
    get_expr_for_vep_consequence_terms_set, \
    get_expr_for_vep_sorted_transcript_consequences_array, \
    get_expr_for_worst_transcript_consequence_annotations_struct, get_expr_for_end_pos, \
    get_expr_for_xpos, get_expr_for_contig, get_expr_for_start_pos, get_expr_for_alt_allele, \
    get_expr_for_ref_allele, get_expr_for_vep_protein_domains_set, get_expr_for_variant_type, \
    get_expr_for_filtering_allele_frequency
from hail_scripts.v01.utils.elasticsearch_utils import VARIANT_GENOTYPE_FIELDS_TO_EXPORT, \
    VARIANT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP, \
    SV_GENOTYPE_FIELDS_TO_EXPORT, SV_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP
from hail_scripts.v01.utils.add_combined_reference_data import add_combined_reference_data_to_vds
from hail_scripts.v01.utils.add_primate_ai import add_primate_ai_to_vds
from hail_scripts.v01.utils.hail_utils import create_hail_context, stop_hail_context
from hail_scripts.v01.utils.validate_vds import validate_vds_genome_version_and_sample_type, validate_vds_has_been_filtered
from hail_scripts.v01.utils.elasticsearch_client import ElasticsearchClient
from hail_scripts.v01.utils.fam_file_utils import MAX_SAMPLES_PER_INDEX, compute_sample_groups_from_fam_file
from hail_scripts.v01.utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr
from hail_scripts.v01.utils.add_1kg_phase3 import add_1kg_phase3_to_vds
from hail_scripts.v01.utils.add_cadd import add_cadd_to_vds
from hail_scripts.v01.utils.add_dbnsfp import add_dbnsfp_to_vds
from hail_scripts.v01.utils.add_clinvar import add_clinvar_to_vds
from hail_scripts.v01.utils.add_exac import add_exac_to_vds
from hail_scripts.v01.utils.add_gnomad import add_gnomad_to_vds
from hail_scripts.v01.utils.add_topmed import add_topmed_to_vds
from hail_scripts.v01.utils.add_mpc import add_mpc_to_vds


logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def init_command_line_args():
    p = argparse.ArgumentParser(description="Pipeline for annotating and loading rare disease callsets into elasticsearch")
    p.add_argument("--genome-version", help="Genome build: 37 or 38", choices=["37", "38"], required=True)

    p.add_argument("--skip-vep", action="store_true", help="Don't run vep.")
    p.add_argument("--skip-annotations", action="store_true", help="Don't add any reference data. Intended for testing.")
    p.add_argument("--skip-validation", action="store_true", help="Don't validate --sample-type and --genome-version. Intended for testing.")
    p.add_argument("--skip-writing-intermediate-vds", action="store_true", help="Skip creating intermediate checkpoints with "
        "write-vds/shut-down-hail-context/restart-hail-context/read-vds cycles which are there to make the pipeline more robust against "
        "crashes due to OOM or issues with preemtible dataproc nodes.")
    p.add_argument('--filter-interval', default="1-MT", help="Only load data in this genomic interval ('chrom1-chrom2' or 'chrom:start-end')")

    p.add_argument('--remap-sample-ids', help="Filepath containing 2 tab-separated columns: current sample id and desired sample id")
    p.add_argument('--subset-samples', help="Filepath containing ids for samples to keep; if used with --remap-sample-ids, ids are the desired ids (post remapping)")
    p.add_argument("--ignore-extra-sample-ids-in-tables", action="store_true")
    p.add_argument("--ignore-extra-sample-ids-in-vds", action="store_true")

    p.add_argument("--fam-file", help=".fam file used to check VDS sample IDs and assign samples to indices with "
                                      "a max of 'num_samples' per index, but making sure that samples from the same family don't end up in different indices. "
                                      "If used with --remap-sample-ids, contains IDs of samples after remapping")
    p.add_argument("--max-samples-per-index", help="Max samples per index. This limit is ignored when --use-nested-objects-for-genotypes is set", type=int, default=MAX_SAMPLES_PER_INDEX)

    p.add_argument('--export-vcf', action="store_true", help="Write out a new VCF file after import")

    p.add_argument("--project-guid", help="seqr project guid", required=True)
    p.add_argument("--family-id", help="(optional) seqr Family id for datasets (such as Manta SV calls) that are generated per-family")
    p.add_argument("--individual-id", help="(optional) seqr Individual id for datasets (such as single-sample Manta SV calls) that are generated per-individual")
    p.add_argument("--sample-type", help="sample type (WES, WGS, RNA)", choices=["WES", "WGS", "RNA"], required=True)
    p.add_argument("--dataset-type", help="what pipeline was used to generate the data", choices=["VARIANTS", "SV"], required=True)
    p.add_argument("--not-gatk-genotypes", action="store_true", help="Use for VARIANTS datasets that have genotype FORMAT other than GT:AD:DP:GQ:PL")

    p.add_argument("--index", help="(optional) elasticsearch index name. If not specified, the index name will be computed based on project_guid, family_id, sample_type and dataset_type.")

    p.add_argument("--host", help="Elastisearch host", default=os.environ.get("ELASTICSEARCH_SERVICE_HOSTNAME", "localhost"))
    p.add_argument("--port", help="Elastisearch port", default="9200")
    p.add_argument("--num-shards", help="Number of index shards", type=int, default=6)

    p.add_argument("--use-temp-loading-nodes", help="Load the new dataset only to temporary loading nodes, then transfer the dataset off of these nodes", action='store_true')
    p.add_argument("--vep-block-size", help="Block size to use for VEP", default=200, type=int)
    p.add_argument("--es-block-size", help="Block size to use when exporting to elasticsearch", default=1000, type=int)
    p.add_argument("--cpu-limit", help="when running locally, limit how many CPUs are used for VEP and other CPU-heavy steps", type=int)

    p.add_argument("--use-nested-objects-for-vep", action="store_true", help="Store vep transcripts as nested objects in elasticsearch.")

    p.add_argument("--use-nested-objects-for-genotypes", action="store_true", help="Store genotypes as nested objects in elasticsearch.")
    p.add_argument("--use-child-docs-for-genotypes", action="store_true", help="Store genotypes as child docs and variant-level annotations as parent docs.")
    p.add_argument("--discard-missing-genotypes", action="store_true", help="Only export called genotypes to nested or child docs.")

    p.add_argument("--exclude-dbnsfp", action="store_true", help="Don't add annotations from dbnsfp. Intended for testing.")
    p.add_argument("--exclude-1kg", action="store_true", help="Don't add 1kg AFs. Intended for testing.")
    p.add_argument("--exclude-omim", action="store_true", help="Don't add OMIM mim id column. Intended for testing.")
    p.add_argument("--exclude-gene-constraint", action="store_true", help="Don't add gene constraint columns. Intended for testing.")
    p.add_argument("--exclude-eigen", action="store_true", help="Don't add Eigen scores. Intended for testing.")
    p.add_argument("--exclude-cadd", action="store_true", help="Don't add CADD scores (they take a really long time to load). Intended for testing.")
    p.add_argument("--exclude-gnomad", action="store_true", help="Don't add gnomAD exome or genome fields. Intended for testing.")
    p.add_argument("--exclude-exac", action="store_true", help="Don't add ExAC fields. Intended for testing.")
    p.add_argument("--exclude-topmed", action="store_true", help="Don't add TopMed AFs. Intended for testing.")
    p.add_argument("--exclude-clinvar", action="store_true", help="Don't add clinvar fields. Intended for testing.")
    p.add_argument("--exclude-hgmd", action="store_true", help="Don't add HGMD fields. Intended for testing.")
    p.add_argument("--exclude-mpc", action="store_true", help="Don't add MPC fields. Intended for testing.")
    p.add_argument("--exclude-primate-ai", action="store_true", help="Don't add PrimateAI fields. Intended for testing.")
    p.add_argument("--exclude-gnomad-coverage", action="store_true", help="Don't add gnomAD exome and genome coverage. Intended for testing.")
    p.add_argument("--exclude-vcf-info-field", action="store_true", help="Don't add any fields from the VCF info field. Intended for testing.")

    p.add_argument("--dont-update-operations-log", action="store_true", help="Don't save metadata about this export in the operations log.")
    p.add_argument("--dont-delete-intermediate-vds-files", action="store_true", help="Keep intermediate VDS files to allow restarting the pipeline "
        "from the middle using --start-with-step")
    p.add_argument("--only-export-to-elasticsearch-at-the-end", action="store_true", help="By default the pipeline first exports intermediate results "
        "and then exports a 2nd set of annotations at the end to reduce the chance of out-of-memory errors. This arg makes it only export the data once "
        "at the end. This is faster if it works, but makes it more likely the pipeline will crash before completing")
    p.add_argument("--create-snapshot", action="store_true", help="Create an elasticsearch snapshot in a google bucket after indexing is complete.")

    p.add_argument("--start-with-step", help="Which pipeline step to start with.", type=int, default=0, choices=[0, 1, 2, 3, 4])
    p.add_argument("--stop-after-step", help="Pipeline will exit after this step.", type=int, default=1000, choices=[0, 1, 2, 3, 4])
    p.add_argument("--start-with-sample-group", help="If the callset contains more samples than the limit specified by --max-samples-per-index, "
        "it will be loaded into multiple separate indices. Setting this command-line arg to a value > 0 causes the pipeline to start from sample "
        "group other than the 1st one. This is useful for restarting a failed pipeline from exactly where it left off.", type=int, default=0)


    p.add_argument("--username", help="(optional) user running this pipeline. This is the local username and it must be passed in because the script can't look it up when it runs on dataproc.")
    p.add_argument("--directory", help="(optional) current directory. This is the local directory and it must be passed in because the script can't look it up when it runs on dataproc.")

    p.add_argument("--output-mt", help="(optional) Output mt filename prefix (eg. test-mt)")
    p.add_argument("input_dataset", help="input VCF or VDS either on the local filesystem or in google cloud. "
        "This can also be a comma-separated list of VCFs for a dataset that's split by chromosome or genomic coordinates (for example: 'input1.vcf.gz,input2.vcf.gz,inputX.vcf.gz').")

    args = p.parse_args()

    if not (args.input_dataset.rstrip("/").endswith(".mt") or args.input_dataset.endswith(".vcf") or args.input_dataset.endswith(".vcf.gz") or args.input_dataset.endswith(".vcf.bgz")):
        p.error("Input must be a .mt or .vcf.gz")

    args.index = compute_index_name(args)

    logger.info("Command args: \n" + " ".join(sys.argv[:1]) + ((" --index " + args.index) if "--index" not in sys.argv else ""))

    logger.info("Parsed args: \n" + pformat(args.__dict__))

    return args

def compute_index_name(args):
    """Returns elasticsearch index name computed based on command-line args"""

    # generate the index name as:  <project>_<WGS_WES>_<family?>_<VARIANTS or SVs>_<YYYYMMDD>_<batch>
    if args.index:
        index_name = args.index.lower()
    else:
        index_name = "%s%s%s__%s__grch%s__%s__%s" % (
            args.project_guid,
            "__"+args.family_id if args.family_id else "",  # optional family id
            "__"+args.individual_id if args.individual_id else "",  # optional individual id
            args.sample_type,
            args.genome_version,
            args.dataset_type,
            time.strftime("%Y%m%d"),
        )

        index_name = index_name.lower()  # elasticsearch requires index names to be all lower-case

    logger.info("Index name: %s" % (index_name,))

    return index_name

def compute_output_mt_prefix(args):
    """Returns output_mt_prefix computed based on command-line args"""

    if args.output_mt:
        output_mt_prefix = os.path.join(os.path.dirname(args.input_dataset), args.output_mt.replace(".mt", ""))
    else:
        if "," in args.input_dataset:
            raise ValueError("Found ',' in input_dataset path, so unable to compute output mt name for saving intermediate results. Please use --output-mt to set output mt prefix.")

        if args.subset_samples:
            output_mt_hash = "__%020d" % abs(hash(",".join(map(str, [args.input_dataset, args.subset_samples, args.remap_sample_ids]))))
        else:
            output_mt_hash = ""
        output_mt_prefix = args.input_dataset.rstrip("/").replace(".vcf", "").replace(".mt", "").replace(".bgz", "").replace(".gz", "").replace(".*", "").replace("*", "") + output_mt_hash

    return output_mt_prefix

def add_global_metadata(mt, args):
    """Adds structured metadata to the mt 'global' struct. This will later be copied to the elasticsearch index _meta field."""

    # Store step0_output_vds as the cached version of the dataset in google buckets, and also set it as the global.sourceFilePath
    # because
    # 1) vep is the most time-consuming step (other than exporting to elasticsearch), so it makes sense to cache results
    # 2) at this stage, all subsetting and remapping has already been applied, so the samples in the dataset are only the ones exported to elasticsearch
    # 3) annotations may be updated / added more often than vep versions.
    mt = mt.annotate_globals(sourceFilePath = args.step0_output_mt)
    mt = mt.annotate_globals(genomeVersion = args.genome_version)
    mt = mt.annotate_globals(sampleType = args.sample_type)
    mt = mt.annotate_globals(datasetType = args.dataset_type)
    return mt

def step0_init_and_run_vep(mt, args):
    if args.start_with_step > 0:
        return mt

    logger.info("\n\n=============================== pipeline - step 0 - run vep ===============================")


    mt = read_in_dataset(
        mt,
        input_path=args.input_dataset.rstrip("/"),
        dataset_type=args.dataset_type,
        filter_interval=args.filter_interval,
        skip_summary=False,
        num_partitions=args.cpu_limit,
        not_gatk_genotypes=args.not_gatk_genotypes,
    )

    # Skip validating, remap/subset samples, and vep for now.
    # validate_dataset(hc, vds, args)
    #
    # vds = remap_samples(hc, vds, args)
    # vds = subset_samples(hc, vds, args)

    mt = add_global_metadata(mt, args)

    # if not args.skip_vep:
    #
    #     vds = run_vep(vds, genome_version=args.genome_version, block_size=args.vep_block_size)
    #     vds = vds.annotate_global_expr('global.gencodeVersion = "{}"'.format("19" if args.genome_version == "37" else "25"))

    if args.step0_output_mt != args.input_dataset.rstrip("/") and not args.skip_writing_intermediate_vds:
        mt.write(args.step0_output_mt, overwrite=True)


    if args.export_vcf:
        logger.info("Writing out to VCF...")
        hl.export_vcf(mt, args.step0_output_vcf)

    args.start_with_step = 1  # step 0 finished, so, if an error occurs and it goes to retry, start with the next step

    return mt

def run_pipeline():
    args = init_command_line_args()

    # compute additional derived params and add them to the args object for convenience
    args.output_mt_prefix = compute_output_mt_prefix(args)

    args.step0_output_vcf = args.output_mt_prefix + (".vep.vcf.bgz" if ".vep" not in args.output_mt_prefix and not args.skip_vep else ".vcf.bgz")
    args.step0_output_mt = args.output_mt_prefix + (".vep.mt" if ".vep" not in args.output_mt_prefix and not args.skip_vep else ".mt")
    args.step1_output_vds = args.output_mt_prefix + ".vep_and_computed_annotations.vds"
    args.step3_output_vds = args.output_mt_prefix + ".vep_and_all_annotations.vds"

    hl.init()

    # args.is_running_locally = hc.sc.master.startswith("local")   # is the pipeline is running locally or on dataproc
    # logger.info("is_running_locally = %s", args.is_running_locally)

    # pipeline steps
    mt = None
    mt = step0_init_and_run_vep(mt, args)
    # hc, vds = step1_compute_derived_fields(hc, vds, args)
    # hc, vds = step2_export_to_elasticsearch(hc, vds, args)
    # hc, vds = step3_add_reference_datasets(hc, vds, args)
    # hc, vds = step4_export_to_elasticsearch(hc, vds, args)
    #
    # if args.stop_after_step > 4:
    #     update_operations_log(args)
    #     cleanup_steps(args)
    #
    # if args.use_temp_loading_nodes:
    #     # move data off of the loading nodes
    #     route_index_to_temp_es_cluster(False, args)

    logger.info("==> Pipeline completed")
    logger.info("")
    logger.info("")
    logger.info("")
    logger.info("========================================================================================================")
    logger.info("")
    logger.info("==> To add this dataset to a seqr project, click 'Edit Datasets' on the seqr project page ")
    logger.info("    (https://seqr.broadinstitute.org/project/{}/project_page)  and enter: ".format(args.project_guid))
    logger.info("")
    logger.info("        Elasticsearch Index: {} ".format(args.index))
    logger.info("        Sample Type: {} ".format(args.sample_type))
    logger.info("        Dataset Path: {} ".format(args.input_dataset))
    logger.info("")
    logger.info("========================================================================================================")
    logger.info("")
    logger.info("")

if __name__ == "__main__":
    run_pipeline()
