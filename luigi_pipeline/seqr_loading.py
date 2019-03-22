import logging

import luigi

from lib.hail_tasks import HailMatrixTableTask, HailElasticSearchTask, GCSorLocalTarget
from lib.global_config import GlobalConfig

logger = logging.getLogger(__name__)


class SeqrValidationError(Exception):
    pass

class SeqrVCFToMTTask(HailMatrixTableTask):
    """
    Inherits from a Hail MT Class to get helper function logic. Main logic to do annotations here.
    """
    reference_mt_path = luigi.Parameter(description='Path to the matrix table storing the reference variants.')
    sample_type = luigi.Parameter(description='Sample type, WGS or WES')
    validate = luigi.BoolParameter(default=True, description='Perform validation on the dataset.')

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
        imputed_stats = HailMatrixTableTask.hl_mt_impute_sample_type(mt, genome_version)

        for name, stat in imputed_stats.items():
            logger.info('Table contains %i out of %i common %s variants.' %
                        (stat['matched_count'], stat['total_count'], name))

        has_coding = imputed_stats['coding']['match']
        has_noncoding = imputed_stats['noncoding']['match']

        if not has_coding and not has_noncoding:
            # No common variants detected.
            raise SeqrValidationError(
                'Genome version validation error: dataset specified as GRCh{genome_version} but doesn\'t contain '
                'the expected number of common GRCh{genome_version} variants'.format(genome_version=genome_version)
            )
        elif has_noncoding and not has_coding:
            # Non coding only.
            raise SeqrValidationError(
                'Dataset is contains noncoding variants but is missing common coding variants for'
                ' GRCh{}'.format(genome_version)
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

    def run(self):
        mt = self.import_vcf()
        if self.validate:
            self.validate_mt(mt, self.genome_version, self.sample_type)
        # TODO: Modify MT.
        mt.write(self.output().path)


class SeqrMTToESTask(HailElasticSearchTask):
    dest_file = luigi.Parameter()

    def requires(self):
        return [SeqrVCFToMTTask()]

    def output(self):
        # TODO: Use https://luigi.readthedocs.io/en/stable/api/luigi.contrib.esindex.html.
        return GCSorLocalTarget(filename=self.dest_file)

    def run(self):
        # Right now it writes to a file, but will export to ES in the future.
        mt = self.import_mt()
        with self.output().open('w') as out_file:
            out_file.write('count: %i' % mt.count()[0])


if __name__ == '__main__':
    luigi.run()
