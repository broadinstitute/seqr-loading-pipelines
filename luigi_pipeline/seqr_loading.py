import logging

import luigi
import hail as hl

from hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient
from lib.hail_tasks import HailMatrixTableTask, HailElasticSearchTask, GCSorLocalTarget, MatrixTableSampleSetError
from lib.model.seqr_mt_schema import SeqrVariantSchema


logger = logging.getLogger(__name__)


class SeqrValidationError(Exception):
    pass

class SeqrVCFToMTTask(HailMatrixTableTask):
    """
    Inherits from a Hail MT Class to get helper function logic. Main logic to do annotations here.
    """
    reference_ht_path = luigi.Parameter(description='Path to the Hail table storing the reference variants.')
    clinvar_ht_path = luigi.Parameter(description='Path to the Hail table storing the clinvar variants.')
    hgmd_ht_path = luigi.Parameter(description='Path to the Hail table storing the hgmd variants.')
    sample_type = luigi.ChoiceParameter(choices=['WGS', 'WES'], description='Sample type, WGS or WES', var_type=str)
    validate = luigi.BoolParameter(default=True, description='Perform validation on the dataset.')
    dataset_type = luigi.ChoiceParameter(choices=['VARIANTS', 'SV'], default='VARIANTS',
                                         description='VARIANTS or SV.')
    remap_path = luigi.OptionalParameter(default=None,
                                         description="Path to a tsv file with two columns: s and seqr_id.")
    subset_path = luigi.OptionalParameter(default=None,
                                          description="Path to a tsv file with one column of sample IDs: s.")

    def run(self):
        mt = self.import_vcf()
        if self.validate:
            self.validate_mt(mt, self.genome_version, self.sample_type)
        if self.remap_path:
            mt = self.remap_sample_ids(mt, self.remap_path)
        if self.subset_path:
            mt = self.subset_samples_and_variants(mt, self.subset_path)
        mt = hl.split_multi(mt)
        mt = HailMatrixTableTask.run_vep(mt, self.genome_version, self.vep_runner)

        ref_data = hl.read_table(self.reference_ht_path)
        clinvar = hl.read_table(self.clinvar_ht_path)
        hgmd = hl.read_table(self.hgmd_ht_path)

        mt = SeqrVariantSchema(mt, ref_data=ref_data, clinvar_data=clinvar, hgmd_data=hgmd).annotate_all(
            overwrite=True).select_annotated_mt()

        mt.describe()
        mt.write(self.output().path)

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
    dest_file = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        # TODO: instead of hardcoded index, generate from project_guid, etc.
        super().__init__(es_index='data', *args, **kwargs)

    def requires(self):
        return [SeqrVCFToMTTask()]

    def output(self):
        # TODO: Use https://luigi.readthedocs.io/en/stable/api/luigi.contrib.esindex.html.
        return GCSorLocalTarget(filename=self.dest_file)

    def run(self):
        schema = SeqrVariantSchema(self.import_mt())
        row_table = schema.elasticsearch_row()
        self.export_table_to_elasticsearch(row_table)

        self.cleanup()

        # This is just for debugging for now. Not needed since the ES export is the output.
        with self.output().open('w') as out_file:
            out_file.write('count: %i' % row_table.count())


if __name__ == '__main__':
    luigi.run()
