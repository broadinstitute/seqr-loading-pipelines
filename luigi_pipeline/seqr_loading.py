import logging
import os
import pprint
import sys

import hail as hl
import luigi
import pkg_resources

from luigi_pipeline.lib.hail_tasks import (
    GCSorLocalTarget,
    HailElasticSearchTask,
    HailMatrixTableTask,
    MatrixTableSampleSetError,
)
from luigi_pipeline.lib.model.seqr_mt_schema import (
    SeqrGenotypesSchema,
    SeqrVariantsAndGenotypesSchema,
    SeqrVariantSchema,
)

logger = logging.getLogger(__name__)

def does_file_exist(path):
    if path.startswith("gs://"):
        return hl.hadoop_exists(path)
    return os.path.exists(path)

def check_if_path_exists(path, label=""):
    if not does_file_exist(path):
        raise ValueError(f"{label} path not found: {path}")

class SeqrValidationError(Exception):
    pass

class SeqrVCFToMTTask(HailMatrixTableTask):
    """
    Inherits from a Hail MT Class to get helper function logic. Main logic to do annotations here.
    """
    reference_ht_path = luigi.Parameter(description='Path to the Hail table storing locus and allele keyed reference data.')
    interval_ref_ht_path = luigi.OptionalParameter(default=None, description='Path to the Hail Table storing interval-keyed reference data.')
    clinvar_ht_path = luigi.Parameter(description='Path to the Hail table storing the clinvar variants.')
    hgmd_ht_path = luigi.OptionalParameter(default=None,
                                   description='Path to the Hail table storing the hgmd variants.')
    sample_type = luigi.ChoiceParameter(choices=['WGS', 'WES'], description='Sample type, WGS or WES', var_type=str)
    dont_validate = luigi.BoolParameter(description='Disable checking whether the dataset matches the specified '
                                                    'genome version and WGS vs. WES sample type.')
    dataset_type = luigi.ChoiceParameter(choices=['VARIANTS', 'SV', 'MITO'], default='VARIANTS',
                                         description='VARIANTS or SV or MITO.')
    remap_path = luigi.OptionalParameter(default=None,
                                         description="Path to a tsv file with two columns: s and seqr_id.")
    subset_path = luigi.OptionalParameter(default=None,
                                          description="Path to a tsv file with one column of sample IDs: s.")
    vep_config_json_path = luigi.OptionalParameter(default=None,
                                        description="Path of hail vep config .json file")
    grch38_to_grch37_ref_chain = luigi.OptionalParameter(default='gs://hail-common/references/grch38_to_grch37.over.chain.gz',
                                        description="Path to GRCh38 to GRCh37 coordinates file")
    hail_temp_dir = luigi.OptionalParameter(default=None, description="Networked temporary directory used by hail for temporary file storage. Must be a network-visible file path.")
    RUN_VEP = True
    SCHEMA_CLASS = SeqrVariantsAndGenotypesSchema

    def run(self):
        if self.hail_temp_dir:
            hl.init(tmp_dir=self.hail_temp_dir) # Need to use the GCP bucket as temp storage for very large callset joins

        # first validate paths
        for source_path in self.source_paths:
            check_if_path_exists(source_path, "source_path")
        if self.dataset_type in set(['VARIANTS', 'MITO']):
            check_if_path_exists(self.reference_ht_path, "reference_ht_path")
            check_if_path_exists(self.clinvar_ht_path, "clinvar_ht_path")
        if self.interval_ref_ht_path: check_if_path_exists(self.interval_ref_ht_path, "interval_ref_ht_path")
        if self.hgmd_ht_path: check_if_path_exists(self.hgmd_ht_path, "hgmd_ht_path")
        if self.remap_path: check_if_path_exists(self.remap_path, "remap_path")
        if self.subset_path: check_if_path_exists(self.subset_path, "subset_path")
        if self.vep_config_json_path: check_if_path_exists(self.vep_config_json_path, "vep_config_json_path")
        if self.grch38_to_grch37_ref_chain: check_if_path_exists(self.grch38_to_grch37_ref_chain, "grch38_to_grch37_ref_chain")
        if self.hail_temp_dir: check_if_path_exists(self.hail_temp_dir, "hail_temp_dir")

        self.read_input_write_mt()

    def get_schema_class_kwargs(self):
        ref = hl.read_table(self.reference_ht_path)
        interval_ref_data = hl.read_table(self.interval_ref_ht_path) if self.interval_ref_ht_path else None
        clinvar_data = hl.read_table(self.clinvar_ht_path)
        # hgmd is optional.
        hgmd = hl.read_table(self.hgmd_ht_path) if self.hgmd_ht_path else None
        return {'ref_data': ref, 'interval_ref_data': interval_ref_data, 'clinvar_data': clinvar_data, 'hgmd_data': hgmd}

    def annotate_globals(self, mt, clinvar_data):
        mt = mt.annotate_globals(sourceFilePath=','.join(self.source_paths),
                                 genomeVersion=self.genome_version,
                                 sampleType=self.sample_type,
                                 datasetType=self.dataset_type,
                                 hail_version=pkg_resources.get_distribution('hail').version)
        if clinvar_data:
            mt = mt.annotate_globals(clinvar_version=clinvar_data.index_globals().version)
        return mt

    def import_dataset(self):
        logger.info("Args:")
        pprint.pprint(self.__dict__)

        return self.import_vcf()

    def read_input_write_mt(self):
        hl._set_flags(use_new_shuffle='1') # Interval ref data join causes shuffle death, this prevents it

        mt = self.import_dataset()
        mt = self.annotate_old_and_split_multi_hts(mt)
        if not self.dont_validate:
            self.validate_mt(mt, self.genome_version, self.sample_type)
        if self.remap_path:
            mt = self.remap_sample_ids(mt, self.remap_path)
        if self.subset_path:
            mt = self.subset_samples_and_variants(mt, self.subset_path)
        if self.genome_version == '38':
            mt = self.add_37_coordinates(mt, self.grch38_to_grch37_ref_chain)
        mt = self.generate_callstats(mt)
        if self.RUN_VEP:
            mt = HailMatrixTableTask.run_vep(mt, self.genome_version, self.vep_runner,
                                             vep_config_json_path=self.vep_config_json_path)

        kwargs = self.get_schema_class_kwargs()
        mt = self.SCHEMA_CLASS(mt, **kwargs).annotate_all(overwrite=True).select_annotated_mt()
        mt = self.annotate_globals(mt, kwargs.get("clinvar_data"))

        mt.describe()
        mt.write(self.output().path, stage_locally=True, overwrite=True)

    def annotate_old_and_split_multi_hts(self, mt):
        """
        Saves the old allele and locus because while split_multi does this, split_multi_hts drops this. Will see if
        we can add this to split_multi_hts and then this will be deprecated.
        :return: mt that has pre-annotations
        """
        # Named `locus_old` instead of `old_locus` because split_multi_hts drops `old_locus`.
        return hl.split_multi_hts(mt.annotate_rows(locus_old=mt.locus, alleles_old=mt.alleles))


class SeqrMTToESTask(HailElasticSearchTask):
    source_paths = luigi.Parameter(default="[]", description='Path or list of paths of VCFs to be loaded.')
    dest_path = luigi.Parameter(description='Path to write the matrix table.')
    genome_version = luigi.Parameter(description='Reference Genome Version (37 or 38)')
    vep_runner = luigi.ChoiceParameter(choices=['VEP', 'DUMMY'], default='VEP', description='Choice of which vep runner to annotate vep.')

    reference_ht_path = luigi.Parameter(default=None, description='Path to the Hail table storing the reference variants.')
    interval_ref_ht_path = luigi.Parameter(default=None, description='Path to the Hail Table storing interval-keyed reference data.')
    clinvar_ht_path = luigi.Parameter(default=None, description='Path to the Hail table storing the clinvar variants.')
    hgmd_ht_path = luigi.OptionalParameter(default=None, description='Path to the Hail table storing the hgmd variants.')
    sample_type = luigi.ChoiceParameter(default="WES", choices=['WGS', 'WES'], description='Sample type, WGS or WES')
    dont_validate = luigi.BoolParameter(description='Disable checking whether the dataset matches the specified '
                                                    'genome version and WGS vs. WES sample type.')
    dataset_type = luigi.ChoiceParameter(choices=['VARIANTS', 'SV', 'MITO'], default='VARIANTS', description='VARIANTS or SV or MITO.')
    remap_path = luigi.OptionalParameter(default=None, description="Path to a tsv file with two columns: s and seqr_id.")
    subset_path = luigi.OptionalParameter(default=None, description="Path to a tsv file with one column of sample IDs: s.")
    vep_config_json_path = luigi.OptionalParameter(default=None, description="Path of hail vep config .json file")
    grch38_to_grch37_ref_chain = luigi.OptionalParameter(default='gs://hail-common/references/grch38_to_grch37.over.chain.gz',
                                        description="Path to GRCh38 to GRCh37 coordinates file")

    def __init__(self, *args, **kwargs):
        # TODO: instead of hardcoded index, generate from project_guid, etc.
        kwargs['source_path'] = self.dest_path
        super().__init__(*args, **kwargs)

        self.completed_marker_path = os.path.join(self.dest_path, '_EXPORTED_TO_ES')

    def requires(self):
        return [SeqrVCFToMTTask(
            source_paths=self.source_paths,
            dest_path=self.dest_path,
            genome_version=self.genome_version,
            vep_runner=self.vep_runner,
            reference_ht_path=self.reference_ht_path,
            interval_ref_ht_path=self.interval_ref_ht_path,
            clinvar_ht_path=self.clinvar_ht_path,
            hgmd_ht_path=self.hgmd_ht_path,
            sample_type=self.sample_type,
            dont_validate=self.dont_validate,
            dataset_type=self.dataset_type,
            remap_path=self.remap_path,
            subset_path=self.subset_path,
            vep_config_json_path=self.vep_config_json_path,
            grch38_to_grch37_ref_chain=self.grch38_to_grch37_ref_chain,
        )]

    def output(self):
        # TODO: Use https://luigi.readthedocs.io/en/stable/api/luigi.contrib.esindex.html.
        return GCSorLocalTarget(filename=self.completed_marker_path)

    def complete(self):
        # Complete is called by Luigi to check if the task is done and will skip if it is.
        # By default it checks to see that the output exists, but we want to check for the
        # _EXPORTED_TO_ES file to make sure it was not terminated halfway.
        return GCSorLocalTarget(filename=self.completed_marker_path).exists()

    def run(self):
        mt = self.import_mt()
        row_table = SeqrVariantsAndGenotypesSchema.elasticsearch_row(mt)
        es_shards = self._mt_num_shards(mt)
        self.export_table_to_elasticsearch(row_table, es_shards)

        with hl.hadoop_open(self.completed_marker_path, "w") as f:
            f.write(".")

        self.cleanup(es_shards)


if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
