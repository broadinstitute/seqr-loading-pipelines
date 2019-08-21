#!/usr/bin/env python

import os

os.system("pip install elasticsearch")

import argparse
from functools import wraps
import hail
import logging
from pprint import pprint, pformat
import time
import sys

from hail_scripts.shared.elasticsearch_utils import ELASTICSEARCH_INDEX, \
    ELASTICSEARCH_UPDATE, ELASTICSEARCH_UPSERT
from hail_scripts.v01.utils.add_hgmd import add_hgmd_to_vds
from hail_scripts.v01.utils.add_eigen import add_eigen_to_vds
from hail_scripts.v01.utils.gcloud_utils import delete_gcloud_file
from hail_scripts.v01.utils.vds_utils import read_in_dataset, compute_minimal_schema, write_vds, run_vep
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
    SV_GENOTYPE_FIELDS_TO_EXPORT, SV_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP, wait_for_loading_shards_transfer
from hail_scripts.v01.utils.add_combined_reference_data import add_combined_reference_data_to_vds
from hail_scripts.v01.utils.add_primate_ai import add_primate_ai_to_vds
from hail_scripts.v01.utils.add_splice_ai import add_splice_ai_to_vds
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
    p.add_argument("--exclude-splice-ai", action="store_true", help="Don't add SpliceAI fields. Intended for testing.")
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

    p.add_argument("--output-vds", help="(optional) Output vds filename prefix (eg. test-vds)")
    p.add_argument("input_dataset", help="input VCF or VDS either on the local filesystem or in google cloud. "
        "This can also be a comma-separated list of VCFs for a dataset that's split by chromosome or genomic coordinates (for example: 'input1.vcf.gz,input2.vcf.gz,inputX.vcf.gz').")

    args = p.parse_args()

    if not (args.input_dataset.rstrip("/").endswith(".vds") or args.input_dataset.endswith(".vcf") or args.input_dataset.endswith(".vcf.gz") or args.input_dataset.endswith(".vcf.bgz")):
        p.error("Input must be a .vds or .vcf.gz")

    args.index = compute_index_name(args)

    logger.info("Command args: \n" + " ".join(sys.argv[:1]) + ((" --index " + args.index) if "--index" not in sys.argv else ""))

    logger.info("Parsed args: \n" + pformat(args.__dict__))

    return args


def timeit(f):
    """Decorator that prints a function's execution time"""

    @wraps(f)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = f(*args, **kwargs)
        end = time.time()
        logger.info('{} runtime: {}'.format(f.__name__, end - start))
        return result
    return wrapper


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


def compute_output_vds_prefix(args):
    """Returns output_vds_prefix computed based on command-line args"""

    if args.output_vds:
        output_vds_prefix = os.path.join(os.path.dirname(args.input_dataset), args.output_vds.replace(".vds", ""))
    else:
        if "," in args.input_dataset:
            raise ValueError("Found ',' in input_dataset path, so unable to compute output vds name for saving intermediate results. Please use --output-vds to set output vds prefix.")

        if args.subset_samples:
            output_vds_hash = "__%020d" % abs(hash(",".join(map(str, [args.input_dataset, args.subset_samples, args.remap_sample_ids]))))
        else:
            output_vds_hash = ""
        output_vds_prefix = args.input_dataset.rstrip("/").replace(".vcf", "").replace(".vds", "").replace(".bgz", "").replace(".gz", "").replace(".*", "").replace("*", "") + output_vds_hash

    return output_vds_prefix


def remap_samples(hc, vds, args):

    if not args.remap_sample_ids:
        return vds

    # NOTE: if sample IDs are remapped first thing, then the fam file should contain the desired (not original IDs)
    logger.info("Remapping sample ids...")
    id_map = hc.import_table(args.remap_sample_ids, no_header=True)
    mapping = dict(zip(id_map.query('f0.collect()'), id_map.query('f1.collect()')))
    # check that ids being remapped exist in VDS
    samples_in_table = set(mapping.keys())
    samples_in_vds = set(vds.sample_ids)
    matched = samples_in_table.intersection(samples_in_vds)
    if len(matched) < len(samples_in_table):
        warning_message = ("Only {0} out of {1} remapping-table IDs matched IDs in the variant callset.\n"
            "Remapping-table IDs that aren't in the VDS: {2}\n"
            "All VDS IDs: {3}").format(
            len(matched), len(samples_in_table), list(samples_in_table.difference(samples_in_vds)), samples_in_vds)
        if not args.ignore_extra_sample_ids_in_tables:
            raise ValueError(warning_message)
        logger.warning(warning_message)
    vds = vds.rename_samples(mapping)
    logger.info('Remapped {} sample ids...'.format(len(matched)))

    return vds


def subset_samples(hc, vds, args):

    if not args.subset_samples:
        return vds

    logger.info("Subsetting to specified samples...")
    keep_samples = hc.import_table(args.subset_samples, no_header=True).key_by('f0')
    # check that all subset samples exist in VDS
    samples_in_table = set(keep_samples.query('f0.collect()'))
    samples_in_vds = set(vds.sample_ids)
    matched = samples_in_table.intersection(samples_in_vds)
    if len(matched) < len(samples_in_table):
        message = ("Only {0} out of {1} subsetting-table IDs matched IDs in the variant callset.\n" 
            "Dropping {2} IDs that aren't in the VDS: {3}\n All VDS IDs: {4}").format(
                len(matched),
                len(samples_in_table),
                len(samples_in_table) - len(matched),
                list(samples_in_table.difference(samples_in_vds)),
                samples_in_vds)
        if not args.ignore_extra_sample_ids_in_tables:
            raise ValueError(message)
        logger.warning(message)

    original_sample_count = vds.num_samples
    vds = vds.filter_samples_table(keep_samples, keep=True).variant_qc().filter_variants_expr('va.qc.AC > 0')
    new_sample_count = vds.num_samples
    logger.info("Finished subsetting samples. Kept {0} out of {1} samples in vds".format(new_sample_count, original_sample_count))

    logger.info("Callset stats after subsetting:")
    pprint(vds.summarize())

    return vds


def compute_sample_groups(vds, args):
    """Computes the lists of sample ids that should be put in the same elasticsearch index(es), making sure
    that the sample ids from the same family end up in the same index, and that the number of samples in an index
    isn't so big that it overloads elasticsearch.

    Args:
        vds (obj): hail vds object
        args (obj): parsed command line args

    Returns:
         list of lists: each list of sample ids should be put into
    """
    if len(vds.sample_ids) > args.max_samples_per_index and not args.use_nested_objects_for_genotypes and not args.use_child_docs_for_genotypes:
        if not args.fam_file:
            raise ValueError("--fam-file must be specified for callsets larger than %s samples. This callset has %s samples." % (args.max_samples_per_index, len(vds.sample_ids)))

        sample_groups = compute_sample_groups_from_fam_file(
            args.fam_file,
            vds.sample_ids,
            args.max_samples_per_index,
            args.ignore_extra_sample_ids_in_vds,
            args.ignore_extra_sample_ids_in_tables,
        )
    else:
        sample_groups = [vds.sample_ids]

    return sample_groups


def add_global_metadata(vds, args):
    """Adds structured metadata to the vds 'global' struct. This will later be copied to the elasticsearch index _meta field."""

    # Store step0_output_vds as the cached version of the dataset in google buckets, and also set it as the global.sourceFilePath
    # because
    # 1) vep is the most time-consuming step (other than exporting to elasticsearch), so it makes sense to cache results
    # 2) at this stage, all subsetting and remapping has already been applied, so the samples in the dataset are only the ones exported to elasticsearch
    # 3) annotations may be updated / added more often than vep versions.
    vds = vds.annotate_global_expr('global.sourceFilePath = "{}"'.format(args.step0_output_vds))
    vds = vds.annotate_global_expr('global.genomeVersion = "{}"'.format(args.genome_version))
    vds = vds.annotate_global_expr('global.sampleType = "{}"'.format(args.sample_type))
    vds = vds.annotate_global_expr('global.datasetType = "{}"'.format(args.dataset_type))

    return vds


def validate_dataset(hc, vds, args):
    if args.skip_validation or args.start_with_step > 0:
        return

    #validate_vds_has_been_filtered(hc, vds)

    validate_vds_genome_version_and_sample_type(hc, vds, args.genome_version, args.sample_type)


def export_to_elasticsearch(
        vds,
        args,
        operation=ELASTICSEARCH_INDEX,
        delete_index_before_exporting=False,
        export_genotypes=True,
        disable_doc_values_for_fields=(),
        disable_index_for_fields=(),
        run_after_index_exists=None,
        force_merge=False,
):
    """Utility method for exporting the given vds to an elasticsearch index."""

    start_with_sample_group = args.start_with_sample_group if args.start_with_step == 0 else 0

    if not export_genotypes:
        genotype_fields_to_export = []
        genotype_field_to_elasticsearch_type_map = {}
    elif args.dataset_type == "VARIANTS":
        genotype_fields_to_export = VARIANT_GENOTYPE_FIELDS_TO_EXPORT
        genotype_field_to_elasticsearch_type_map = VARIANT_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP
    elif args.dataset_type == "SV":
        genotype_fields_to_export = SV_GENOTYPE_FIELDS_TO_EXPORT
        genotype_field_to_elasticsearch_type_map = SV_GENOTYPE_FIELD_TO_ELASTICSEARCH_TYPE_MAP
    else:
        raise ValueError("Unexpected args.dataset_type: %s" % args.dataset_type)

    vds = vds.persist()

    sample_groups = compute_sample_groups(vds, args)
    client = ElasticsearchClient(args.host, args.port)
    for i, sample_group in enumerate(sample_groups):

        if i < start_with_sample_group:
            continue

        #if delete_index_before_exporting and i < 4:
        #    continue

        if len(sample_groups) > 1:
            vds_sample_subset = vds.filter_samples_list(sample_group, keep=True)
            current_index_name = "%s_%s" % (args.index, i)
        else:
            vds_sample_subset = vds
            current_index_name = args.index

        logger.info("==> exporting %s samples into %s" % (len(sample_group), current_index_name))
        logger.info("Samples: %s .. %s" % (", ".join(sample_group[:3]), ", ".join(sample_group[-3:])))

        logger.info("==> export to elasticsearch - vds schema:\n" + pformat(vds.variant_schema))

        timestamp1 = time.time()

        client.export_vds_to_elasticsearch(
            vds_sample_subset,
            genotype_fields_to_export=genotype_fields_to_export,
            genotype_field_to_elasticsearch_type_map=genotype_field_to_elasticsearch_type_map,
            export_genotypes_as_nested_field=bool(args.use_nested_objects_for_genotypes),
            export_genotypes_as_child_docs=bool(args.use_child_docs_for_genotypes),
            discard_missing_genotypes=bool(args.discard_missing_genotypes),
            index_name=current_index_name,
            index_type_name="variant",
            block_size=args.es_block_size,
            num_shards=args.num_shards,
            delete_index_before_exporting=delete_index_before_exporting,
            elasticsearch_write_operation=operation,
            elasticsearch_mapping_id="docId",
            disable_doc_values_for_fields=disable_doc_values_for_fields,
            disable_index_for_fields=disable_index_for_fields,
            is_split_vds=True,
            run_after_index_exists=run_after_index_exists,
            verbose=False,
            force_merge=force_merge,
        )

        timestamp2 = time.time()
        logger.info("==> finished exporting - time: %s seconds" % (timestamp2 - timestamp1))


@timeit
def step0_init_and_run_vep(hc, vds, args):
    if args.start_with_step > 0:
        return hc, vds

    logger.info("\n\n=============================== pipeline - step 0 - run vep ===============================")

    vds = read_in_dataset(
        hc,
        input_path=args.input_dataset.rstrip("/"),
        dataset_type=args.dataset_type,
        filter_interval=args.filter_interval,
        skip_summary=False,
        num_partitions=args.cpu_limit,
        not_gatk_genotypes=args.not_gatk_genotypes,
    )

    validate_dataset(hc, vds, args)

    vds = remap_samples(hc, vds, args)
    vds = subset_samples(hc, vds, args)

    vds = add_global_metadata(vds, args)

    if not args.skip_vep:

        vds = run_vep(vds, genome_version=args.genome_version, block_size=args.vep_block_size)
        vds = vds.annotate_global_expr('global.gencodeVersion = "{}"'.format("19" if args.genome_version == "37" else "25"))

    if args.step0_output_vds != args.input_dataset.rstrip("/") and not args.skip_writing_intermediate_vds:
        write_vds(vds, args.step0_output_vds)

    if args.export_vcf:
        logger.info("Writing out to VCF...")
        vds.export_vcf(args.step0_output_vcf, overwrite=True)

    args.start_with_step = 1  # step 0 finished, so, if an error occurs and it goes to retry, start with the next step

    return hc, vds


@timeit
def step1_compute_derived_fields(hc, vds, args):
    if args.start_with_step > 1 or args.stop_after_step < 1:
        return hc, vds

    logger.info("\n\n=============================== pipeline - step 1 - compute derived fields ===============================")

    if vds is None or not args.skip_writing_intermediate_vds:
        stop_hail_context(hc)
        hc = create_hail_context()
        vds = read_in_dataset(hc, args.step0_output_vds, dataset_type=args.dataset_type, skip_summary=True, num_partitions=args.cpu_limit)

    parallel_computed_annotation_exprs = [
        "va.docId = %s" % get_expr_for_variant_id(512),
        "va.variantId = %s" % get_expr_for_variant_id(),
        "va.variantType= %s" % get_expr_for_variant_type(),
        "va.contig = %s" % get_expr_for_contig(),
        "va.pos = %s" % get_expr_for_start_pos(),
        "va.start = %s" % get_expr_for_start_pos(),
        "va.end = %s" % get_expr_for_end_pos(),
        "va.ref = %s" % get_expr_for_ref_allele(),
        "va.alt = %s" % get_expr_for_alt_allele(),
        "va.xpos = %s" % get_expr_for_xpos(pos_field="start"),
        "va.xstart = %s" % get_expr_for_xpos(pos_field="start"),
        "va.sortedTranscriptConsequences = %s" % get_expr_for_vep_sorted_transcript_consequences_array(
            vep_root="va.vep",
            include_coding_annotations=True,
            add_transcript_rank=bool(args.use_nested_objects_for_vep)),
    ]

    if args.dataset_type == "VARIANTS":
        FAF_CONFIDENCE_INTERVAL = 0.95  # based on https://macarthurlab.slack.com/archives/C027LHMPP/p1528132141000430

        parallel_computed_annotation_exprs += [
            "va.FAF = %s" % get_expr_for_filtering_allele_frequency("va.info.AC[va.aIndex - 1]", "va.info.AN", FAF_CONFIDENCE_INTERVAL),
        ]

    serial_computed_annotation_exprs = [
        "va.xstop = %s" % get_expr_for_xpos(field_prefix="va.", pos_field="end"),
        "va.transcriptIds = %s" % get_expr_for_vep_transcript_ids_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences"),
        "va.domains = %s" % get_expr_for_vep_protein_domains_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences"),
        "va.transcriptConsequenceTerms = %s" % get_expr_for_vep_consequence_terms_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences"),
        "va.mainTranscript = %s" % get_expr_for_worst_transcript_consequence_annotations_struct("va.sortedTranscriptConsequences"),
        "va.geneIds = %s" % get_expr_for_vep_gene_ids_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences"),
        "va.codingGeneIds = %s" % get_expr_for_vep_gene_ids_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences", only_coding_genes=True),
    ]

    # serial_computed_annotation_exprs += [
    #   "va.sortedTranscriptConsequences = va.sortedTranscriptConsequences.map(c => drop(c, amino_acids, biotype))"
    #]

    if not bool(args.use_nested_objects_for_vep):
        serial_computed_annotation_exprs += [
            "va.sortedTranscriptConsequences = json(va.sortedTranscriptConsequences)"
        ]

    vds = vds.annotate_variants_expr(parallel_computed_annotation_exprs)

    for expr in serial_computed_annotation_exprs:
        vds = vds.annotate_variants_expr(expr)

    pprint(vds.variant_schema)

    INPUT_SCHEMA  = {}
    if args.dataset_type == "VARIANTS":
        INPUT_SCHEMA["top_level_fields"] = """
            docId: String,
            variantId: String,
            originalAltAlleles: Set[String],

            contig: String,
            start: Int,
            pos: Int,
            end: Int,
            ref: String,
            alt: String,

            xpos: Long,
            xstart: Long,
            xstop: Long,

            rsid: String,
            --- qual: Double,
            filters: Set[String],
            aIndex: Int,

            geneIds: Set[String],
            transcriptIds: Set[String],
            codingGeneIds: Set[String],
            domains: Set[String],
            transcriptConsequenceTerms: Set[String],
            sortedTranscriptConsequences: String,
            mainTranscript: Struct,
        """

        if args.not_gatk_genotypes:
            INPUT_SCHEMA["info_fields"] = """
                AC: Array[Int],
                AF: Array[Double],
                AN: Int,
                --- BaseQRankSum: Double,
                --- ClippingRankSum: Double,
                --- DP: Int,
                --- FS: Double,
                --- InbreedingCoeff: Double,
                --- MQ: Double,
                --- MQRankSum: Double,
                --- QD: Double,
                --- ReadPosRankSum: Double,
                --- VQSLOD: Double,
                --- culprit: String,
            """
        else:
            INPUT_SCHEMA["info_fields"] = """
                AC: Array[Int],
                AF: Array[Double],
                AN: Int,
                --- BaseQRankSum: Double,
                --- ClippingRankSum: Double,
                --- DP: Int,
                --- FS: Double,
                --- InbreedingCoeff: Double,
                --- MQ: Double,
                --- MQRankSum: Double,
                --- QD: Double,
                --- ReadPosRankSum: Double,
                --- VQSLOD: Double,
                --- culprit: String,
            """
    elif args.dataset_type == "SV":
        INPUT_SCHEMA["top_level_fields"] = """
            docId: String,
            variantId: String,

            contig: String,
            start: Int,
            pos: Int,
            end: Int,
            ref: String,
            alt: String,

            xpos: Long,
            xstart: Long,
            xstop: Long,

            rsid: String,
            --- qual: Double,
            filters: Set[String],
            aIndex: Int,
            
            geneIds: Set[String],
            transcriptIds: Set[String],
            codingGeneIds: Set[String],
            domains: Set[String],
            transcriptConsequenceTerms: Set[String],
            sortedTranscriptConsequences: String,
            mainTranscript: Struct,
        """

        # END=100371979;SVTYPE=DEL;SVLEN=-70;CIGAR=1M70D	GT:FT:GQ:PL:PR:SR
        INPUT_SCHEMA["info_fields"] = """
            IMPRECISE: Boolean,
            SVTYPE: String,
            SVLEN: Int,
            END: Int,
            --- OCC: Int,
            --- FRQ: Double,
        """
    else:
        raise ValueError("Unexpected dataset_type: %s" % args.dataset_type)

    if args.exclude_vcf_info_field:
        INPUT_SCHEMA["info_fields"] = ""

    expr = convert_vds_schema_string_to_annotate_variants_expr(root="va.clean", **INPUT_SCHEMA)

    vds = vds.annotate_variants_expr(expr=expr)
    vds = vds.annotate_variants_expr("va = va.clean")

    if not args.skip_writing_intermediate_vds:
        write_vds(vds, args.step1_output_vds)

    args.start_with_step = 2  # step 1 finished, so, if an error occurs and it goes to retry, start with the next step

    return hc, vds


@timeit
def step2_export_to_elasticsearch(hc, vds, args):
    if args.start_with_step > 2 or args.stop_after_step < 2 or args.only_export_to_elasticsearch_at_the_end:
        return hc, vds

    logger.info("\n\n=============================== pipeline - step 2 - export to elasticsearch ===============================")

    if vds is None or not args.skip_writing_intermediate_vds:
        stop_hail_context(hc)
        hc = create_hail_context()
        vds = read_in_dataset(hc, args.step1_output_vds, dataset_type=args.dataset_type, skip_summary=True, num_partitions=args.cpu_limit)

    export_to_elasticsearch(
        vds,
        args,
        operation=ELASTICSEARCH_UPSERT,
        delete_index_before_exporting=True,
        export_genotypes=True,
        disable_doc_values_for_fields=("sortedTranscriptConsequences", ) if not bool(args.use_nested_objects_for_vep) else (),
        disable_index_for_fields=("sortedTranscriptConsequences", ) if not bool(args.use_nested_objects_for_vep) else (),
        run_after_index_exists=(lambda: route_index_to_temp_es_cluster(True, args)) if args.use_temp_loading_nodes else None,
    )

    args.start_with_step = 3   # step 2 finished, so, if an error occurs and it goes to retry, start with the next step

    return hc, vds


@timeit
def step3_add_reference_datasets(hc, vds, args):
    if args.start_with_step > 3 or args.stop_after_step < 3:
        return hc, vds

    logger.info("\n\n=============================== pipeline - step 3 - add reference datasets ===============================")

    if vds is None or not args.skip_writing_intermediate_vds:
        stop_hail_context(hc)
        hc = create_hail_context()
        vds = read_in_dataset(hc, args.step1_output_vds, dataset_type=args.dataset_type, skip_summary=True)

    if not args.only_export_to_elasticsearch_at_the_end:

        vds = compute_minimal_schema(vds, args.dataset_type)

    if args.dataset_type == "VARIANTS":
        # annotate with the combined reference data file which was generated using
        # ../download_and_create_reference_datasets/v01/hail_scripts/combine_all_variant_level_reference_data.py
        # and contains all these annotations in one .vds

        if not (args.exclude_dbnsfp or args.exclude_cadd or args.exclude_1kg or args.exclude_exac or
                args.exclude_topmed or args.exclude_mpc or args.exclude_gnomad or args.exclude_eigen or
                args.exclude_primate_ai or args.exclude_splice_ai):

            logger.info("\n==> add combined variant-level reference data")
            vds = add_combined_reference_data_to_vds(hc, vds, args.genome_version, subset=args.filter_interval)

        else:
            # annotate with each reference data file - one-by-one
            if not args.skip_annotations and not args.exclude_dbnsfp:
                logger.info("\n==> add dbnsfp")
                vds = add_dbnsfp_to_vds(hc, vds, args.genome_version, root="va.dbnsfp", subset=args.filter_interval)

            if not args.skip_annotations and not args.exclude_cadd:
                logger.info("\n==> add cadd")
                vds = add_cadd_to_vds(hc, vds, args.genome_version, root="va.cadd", subset=args.filter_interval)

            if not args.skip_annotations and not args.exclude_1kg:
                logger.info("\n==> add 1kg")
                vds = add_1kg_phase3_to_vds(hc, vds, args.genome_version, root="va.g1k", subset=args.filter_interval)

            if not args.skip_annotations and not args.exclude_exac:
                logger.info("\n==> add exac")
                vds = add_exac_to_vds(hc, vds, args.genome_version, root="va.exac", subset=args.filter_interval)

            if not args.skip_annotations and not args.exclude_topmed:
                logger.info("\n==> add topmed")
                vds = add_topmed_to_vds(hc, vds, args.genome_version, root="va.topmed", subset=args.filter_interval)

            if not args.skip_annotations and not args.exclude_mpc:
                logger.info("\n==> add mpc")
                vds = add_mpc_to_vds(hc, vds, args.genome_version, root="va.mpc", subset=args.filter_interval)

            if not args.skip_annotations and not args.exclude_gnomad:
                logger.info("\n==> add gnomad exomes")
                vds = add_gnomad_to_vds(hc, vds, args.genome_version, exomes_or_genomes="exomes", root="va.gnomad_exomes", subset=args.filter_interval)

            if not args.skip_annotations and not args.exclude_gnomad:
                logger.info("\n==> add gnomad genomes")
                vds = add_gnomad_to_vds(hc, vds, args.genome_version, exomes_or_genomes="genomes", root="va.gnomad_genomes", subset=args.filter_interval)

            if not args.skip_annotations and not args.exclude_eigen:
                logger.info("\n==> add eigen")
                vds = add_eigen_to_vds(hc, vds, args.genome_version, root="va.eigen", subset=args.filter_interval)

            if not args.exclude_primate_ai:
                logger.info("\n==> add primate_ai")
                vds = add_primate_ai_to_vds(hc, vds, args.genome_version, root="va.primate_ai", subset=args.filter_interval)

            if not args.exclude_splice_ai:
                logger.info("\n==> add splice_ai")
                vds = add_splice_ai_to_vds(hc, vds, args.genome_version, root="va.splice_ai", subset=args.filter_interval)

    if not args.skip_annotations and not args.exclude_clinvar:
        logger.info("\n==> add clinvar")
        vds = add_clinvar_to_vds(hc, vds, args.genome_version, root="va.clinvar", subset=args.filter_interval)

    if not args.skip_annotations and not args.exclude_hgmd:
        logger.info("\n==> add hgmd")
        vds = add_hgmd_to_vds(hc, vds, args.genome_version, root="va.hgmd", subset=args.filter_interval)

    if not args.skip_writing_intermediate_vds:
        write_vds(vds, args.step3_output_vds)

    args.start_with_step = 4   # step 3 finished, so, if an error occurs and it goes to retry, start with the next step

    return hc, vds


@timeit
def step4_export_to_elasticsearch(hc, vds, args):
    if args.start_with_step > 4 or args.stop_after_step < 4:
        return hc, vds

    logger.info("\n\n=============================== pipeline - step 4 - export to elasticsearch ===============================")

    if vds is None or (not args.is_running_locally and not args.skip_writing_intermediate_vds):
        stop_hail_context(hc)
        hc = create_hail_context()
        vds = read_in_dataset(hc, args.step3_output_vds, dataset_type=args.dataset_type, skip_summary=True, num_partitions=args.cpu_limit)

    export_to_elasticsearch(
        vds,
        args,
        operation=ELASTICSEARCH_UPDATE if not args.only_export_to_elasticsearch_at_the_end else ELASTICSEARCH_INDEX,
        delete_index_before_exporting=args.only_export_to_elasticsearch_at_the_end,
        export_genotypes=args.only_export_to_elasticsearch_at_the_end,
        disable_doc_values_for_fields=("sortedTranscriptConsequences",) if not bool(args.use_nested_objects_for_vep) else (),
        disable_index_for_fields=("sortedTranscriptConsequences",) if not bool(args.use_nested_objects_for_vep) else (),
        run_after_index_exists=(lambda: route_index_to_temp_es_cluster(True, args)) if args.use_temp_loading_nodes else None,
        force_merge=True,
    )

    args.start_with_step = 5   # step 4 finished, so, if an error occurs and it goes to retry, start with the next step

    return hc, vds


def update_operations_log(args):
    if args.dont_update_operations_log:
        return

    logger.info("==> update operations log")
    client = ElasticsearchClient(args.host, args.port)
    client.save_index_operation_metadata(
        args.input_dataset,
        args.index,
        args.genome_version,
        fam_file=args.fam_file,
        remap_sample_ids=args.remap_sample_ids,
        subset_samples=args.subset_samples,
        skip_vep=args.skip_vep,
        project_id=args.project_guid,
        dataset_type=args.dataset_type,
        sample_type=args.sample_type,
        command=" ".join(sys.argv),
        directory=args.directory,
        username=args.username,
        operation="create_index",
        status="success",
    )


def cleanup_steps(args):
    if args.dont_delete_intermediate_vds_files:
        return

    logger.info("==> delete intermediate vds files")
    #delete_gcloud_file(step0_output_vds) -- don't delete since it's saved as the sourceFile in the index and seqr Sample records
    if args.step1_output_vds.startswith("gs://"):
        delete_gcloud_file(args.step1_output_vds)
    if args.step3_output_vds.startswith("gs://"):
        delete_gcloud_file(args.step3_output_vds)


def route_index_to_temp_es_cluster(yes, args):
    """Apply shard allocation filtering rules for the given index to elasticsearch data nodes with *loading* in their name:

    If yes is True, route new documents in the given index only to nodes named "*loading*".
    Otherwise, move any shards in this index off of nodes named "*loading*"

    Args:
        yes (bool): whether to route shards in the given index to the "*loading*" nodes, or move shards off of these nodes.
        args: args from ArgumentParser - used to compute the index name and get elasticsearch host and port.
    """
    if yes:
        require_name = "es-data-loading*"
        exclude_name = ""
    else:
        require_name = ""
        exclude_name = "es-data-loading*"

    body = {
        "index.routing.allocation.require._name": require_name,
        "index.routing.allocation.exclude._name": exclude_name
    }

    logger.info("==> Setting {}* settings = {}".format(args.index, body))

    index_arg = "{}*".format(args.index)
    client = ElasticsearchClient(args.host, args.port)
    client.es.indices.put_settings(index=index_arg, body=body)

    if not yes:
        wait_for_loading_shards_transfer(client, index=index_arg)


def run_pipeline():
    args = init_command_line_args()

    # compute additional derived params and add them to the args object for convenience
    args.output_vds_prefix = compute_output_vds_prefix(args)

    args.step0_output_vcf = args.output_vds_prefix + (".vep.vcf.bgz" if ".vep" not in args.output_vds_prefix and not args.skip_vep else ".vcf.bgz")
    args.step0_output_vds = args.output_vds_prefix + (".vep.vds" if ".vep" not in args.output_vds_prefix and not args.skip_vep else ".vds")
    args.step1_output_vds = args.output_vds_prefix + ".vep_and_computed_annotations.vds"
    args.step3_output_vds = args.output_vds_prefix + ".vep_and_all_annotations.vds"

    hc = create_hail_context()

    args.is_running_locally = hc.sc.master.startswith("local")   # is the pipeline is running locally or on dataproc
    logger.info("is_running_locally = %s", args.is_running_locally)

    # pipeline steps
    vds = None
    hc, vds = step0_init_and_run_vep(hc, vds, args)
    hc, vds = step1_compute_derived_fields(hc, vds, args)
    hc, vds = step2_export_to_elasticsearch(hc, vds, args)
    hc, vds = step3_add_reference_datasets(hc, vds, args)
    hc, vds = step4_export_to_elasticsearch(hc, vds, args)
    
    if args.stop_after_step > 4:
        update_operations_log(args)
        cleanup_steps(args)

        if args.use_temp_loading_nodes:
            # move data off of the loading nodes
            route_index_to_temp_es_cluster(False, args)

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
