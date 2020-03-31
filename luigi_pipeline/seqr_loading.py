import logging
import os
import pkg_resources
import pprint
import sys

import luigi
import hail as hl

from lib.hail_tasks import HailMatrixTableTask, HailElasticSearchTask, GCSorLocalTarget, MatrixTableSampleSetError
from lib.model.seqr_mt_schema import SeqrVariantSchema, SeqrGenotypesSchema, SeqrVariantsAndGenotypesSchema

logger = logging.getLogger(__name__)


def check_if_path_exists(path, label=""):
    if (path.startswith("gs://") and not hl.hadoop_exists(path)) or (not path.startswith("gs://") and not os.path.exists(path)):
        raise ValueError(f"{label} path not found: {path}")


class SeqrValidationError(Exception):
    pass

class SeqrVCFToMTTask(HailMatrixTableTask):
    """
    Inherits from a Hail MT Class to get helper function logic. Main logic to do annotations here.
    """
    reference_ht_path = luigi.Parameter(description='Path to the Hail table storing the reference variants.')
    clinvar_ht_path = luigi.Parameter(description='Path to the Hail table storing the clinvar variants.')
    hgmd_ht_path = luigi.Parameter(default=None,
                                   description='Path to the Hail table storing the hgmd variants.')
    sample_type = luigi.ChoiceParameter(choices=['WGS', 'WES'], description='Sample type, WGS or WES', var_type=str)
    validate = luigi.BoolParameter(default=False, description='Perform validation on the dataset.')
    dataset_type = luigi.ChoiceParameter(choices=['VARIANTS', 'SV'], default='VARIANTS',
                                         description='VARIANTS or SV.')
    remap_path = luigi.OptionalParameter(default=None,
                                         description="Path to a tsv file with two columns: s and seqr_id.")
    subset_path = luigi.OptionalParameter(default=None,
                                          description="Path to a tsv file with one column of sample IDs: s.")
    vep_config_json_path = luigi.OptionalParameter(default=None,
                                        description="Path of hail vep config .json file")

    def run(self):
        # first validate paths
        for source_path in self.source_paths:
            check_if_path_exists(source_path, "source_path")
        check_if_path_exists(self.reference_ht_path, "reference_ht_path")
        if self.clinvar_ht_path: check_if_path_exists(self.clinvar_ht_path, "clinvar_ht_path")
        if self.hgmd_ht_path: check_if_path_exists(self.hgmd_ht_path, "hgmd_ht_path")
        if self.remap_path: check_if_path_exists(self.remap_path, "remap_path")
        if self.subset_path: check_if_path_exists(self.subset_path, "subset_path")
        if self.vep_config_json_path: check_if_path_exists(self.vep_config_json_path, "vep_config_json_path")

        self.read_vcf_write_mt()

    def read_vcf_write_mt(self, schema_cls=SeqrVariantsAndGenotypesSchema):
        logger.info("Args:")
        pprint.pprint(self.__dict__)

        mt = self.import_vcf()
        mt = self.annotate_old_and_split_multi_hts(mt)
        if self.validate:
            self.validate_mt(mt, self.genome_version, self.sample_type)
        if self.remap_path:
            mt = self.remap_sample_ids(mt, self.remap_path)
        if self.subset_path:
            mt = self.subset_samples_and_variants(mt, self.subset_path)
        mt = HailMatrixTableTask.run_vep(mt, self.genome_version, self.vep_runner, vep_config_json_path=self.vep_config_json_path)

        ref_data = hl.read_table(self.reference_ht_path)
        clinvar = hl.read_table(self.clinvar_ht_path)
        # hgmd is optional.
        hgmd = hl.read_table(self.hgmd_ht_path) if self.hgmd_ht_path else None

        mt = schema_cls(mt, ref_data=ref_data, clinvar_data=clinvar, hgmd_data=hgmd).annotate_all(
            overwrite=True).select_annotated_mt()

        mt = mt.annotate_globals(sourceFilePath=','.join(self.source_paths),
                                 genomeVersion=self.genome_version,
                                 sampleType=self.sample_type,
                                 hail_version=pkg_resources.get_distribution('hail').version)

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

    @staticmethod
    def validate_mt(mt, genome_version, sample_type):
        """
        Validate the mt by checking against a list of common coding and non-coding variants given its
        genome version. This validates genome_version, variants, and the reported sample type.

        :param mt: mt to validate
        :param genome_version: reference genome version
        :param sample_type: WGS or WES
        :return: True or Exception
        """
        sample_type_stats = HailMatrixTableTask.sample_type_stats(mt, genome_version)

        for name, stat in sample_type_stats.items():
            logger.info('Table contains %i out of %i common %s variants.' %
                        (stat['matched_count'], stat['total_count'], name))

        has_coding = sample_type_stats['coding']['match']
        has_noncoding = sample_type_stats['noncoding']['match']

        if not has_coding and not has_noncoding:
            # No common variants detected.
            raise SeqrValidationError(
                'Genome version validation error: dataset specified as GRCh{genome_version} but doesn\'t contain '
                'the expected number of common GRCh{genome_version} variants'.format(genome_version=genome_version)
            )
        elif has_noncoding and not has_coding:
            # Non coding only.
            raise SeqrValidationError(
                'Sample type validation error: Dataset contains noncoding variants but is missing common coding '
                'variants for GRCh{}. Please verify that the dataset contains coding variants.' .format(genome_version)
            )
        elif has_coding and not has_noncoding:
            # Only coding should be WES.
            if sample_type != 'WES':
                raise SeqrValidationError(
                    'Sample type validation error: dataset sample-type is specified as {} but appears to be '
                    'WGS because it contains many common coding variants'.format(sample_type)
                )
        elif has_noncoding and has_coding:
            # Both should be WGS.
            if sample_type != 'WGS':
                raise SeqrValidationError(
                    'Sample type validation error: dataset sample-type is specified as {} but appears to be '
                    'WES because it contains many common non-coding variants'.format(sample_type)
                )
        return True


class SeqrMTToESTask(HailElasticSearchTask):
    source_paths = luigi.Parameter(description='Path or list of paths of VCFs to be loaded.')
    dest_path = luigi.Parameter(description='Path to write the matrix table.')
    genome_version = luigi.Parameter(description='Reference Genome Version (37 or 38)')
    vep_runner = luigi.ChoiceParameter(choices=['VEP', 'DUMMY'], default='VEP', description='Choice of which vep runner to annotate vep.')

    reference_ht_path = luigi.Parameter(default=None, description='Path to the Hail table storing the reference variants.')
    clinvar_ht_path = luigi.Parameter(default=None, description='Path to the Hail table storing the clinvar variants.')
    hgmd_ht_path = luigi.Parameter(default=None, description='Path to the Hail table storing the hgmd variants.')
    sample_type = luigi.ChoiceParameter(default="WES", choices=['WGS', 'WES'], description='Sample type, WGS or WES')
    validate = luigi.BoolParameter(default=False, description='Perform validation on the dataset.')
    dataset_type = luigi.ChoiceParameter(choices=['VARIANTS', 'SV'], default='VARIANTS', description='VARIANTS or SV.')
    remap_path = luigi.OptionalParameter(default=None, description="Path to a tsv file with two columns: s and seqr_id.")
    subset_path = luigi.OptionalParameter(default=None, description="Path to a tsv file with one column of sample IDs: s.")
    vep_config_json_path = luigi.OptionalParameter(default=None, description="Path of hail vep config .json file")

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
            clinvar_ht_path=self.clinvar_ht_path,
            hgmd_ht_path=self.hgmd_ht_path,
            sample_type=self.sample_type,
            validate=self.validate,
            dataset_type=self.dataset_type,
            remap_path=self.remap_path,
            subset_path=self.subset_path,
            vep_config_json_path=self.vep_config_json_path,
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
        self.export_table_to_elasticsearch(row_table, self._mt_num_shards(mt))

        with hl.hadoop_open(self.completed_marker_path, "w") as f:
            f.write(".")

        self.cleanup()


if __name__ == '__main__':
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
