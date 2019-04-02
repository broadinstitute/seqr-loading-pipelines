"""
Tasks for Hail.
"""
import hail as hl
import luigi
from luigi.contrib import gcs

from lib.global_config import GlobalConfig
import lib.hail_vep_runners as vep_runners
from hail_scripts.v02.utils import hail_utils


def GCSorLocalTarget(filename):
    target = gcs.GCSTarget if filename.startswith('gs://') else luigi.LocalTarget
    return target(filename)


class VcfFile(luigi.Task):
    filename = luigi.Parameter()
    def output(self):
       return GCSorLocalTarget(self.filename)


class HailMatrixTableTask(luigi.Task):
    """
    Task that reads in list of VCFs and writes as a Matrix Table. To be overwritten
    to provide specific operations.
    Does not run if dest path exists (complete) or the source path does not (fail).
    """
    source_paths = luigi.ListParameter(description='List of paths to VCFs to be loaded.')
    dest_path = luigi.Parameter(description='Path to write the matrix table.')
    genome_version = luigi.Parameter(description='Reference Genome Version (37 or 38)')
    vep_runner = luigi.ChoiceParameter(choices=['VEP', 'DUMMY'], default='VEP', description='Choice of which vep runner'
                                                                                            'to annotate vep.')

    def requires(self):
        return [VcfFile(filename=s) for s in self.source_paths]

    def output(self):
        # TODO: Look into checking against the _SUCCESS file in the mt.
        return GCSorLocalTarget(self.dest_path)

    def run(self):
        # Overwrite to do custom transformations.
        mt = self.import_vcf()
        mt.write(self.output().path)

    def import_vcf(self):
        # Import the VCFs from inputs.
        return hl.import_vcf([vcf_file.path for vcf_file in self.input()],
                             force_bgz=True)

    @staticmethod
    def sample_type_stats(mt, genome_version, threshold=0.3):
        """
        Calculate stats for sample type by chcking against a list of common coding and non-coding variants.
        If the match for each respective type is over the threshold, we return a match.

        :param mt: Matrix Table to check
        :param genome_version: reference genome version
        :param threshold: if the matched percentage is over this threshold, we classify as match
        :return: a dict of coding/non-coding to dict with 'matched_count', 'total_count' and 'match' boolean.
        """
        stats = {}
        types_to_ht_path = {
            'noncoding': GlobalConfig().param_kwargs['validation_%s_noncoding_ht' % genome_version],
            'coding': GlobalConfig().param_kwargs['validation_%s_coding_ht' % genome_version]
        }
        for sample_type, ht_path in types_to_ht_path.items():
            ht = hl.read_table(ht_path)
            stats[sample_type] = ht_stats = {
                'matched_count': mt.semi_join_rows(ht).count_rows(),
                'total_count': ht.count(),

            }
            ht_stats['match'] = (ht_stats['matched_count']/ht_stats['total_count']) >= threshold
        return stats

    @staticmethod
    def run_vep(mt, genome_version, runner='VEP'):
        runners = {
            'VEP': vep_runners.HailVEPRunner,
            'DUMMY': vep_runners.HailVEPDummyRunner
        }

        return runners[runner]().run(mt, genome_version)


class HailElasticSearchTask(luigi.Task):
    """
    Loads a MT to ES (TODO).
    """
    source_path = luigi.OptionalParameter(default=None)

    def requires(self):
        return [VcfFile(filename=self.source_path)]

    def run(self):
        mt = self.import_mt()
        # TODO: Load into ES

    def import_mt(self):
        return hl.read_matrix_table(self.input()[0].path)
