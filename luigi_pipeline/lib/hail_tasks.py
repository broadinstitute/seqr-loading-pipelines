"""
Tasks for Hail.
"""
import hail as hl
import luigi
from luigi.contrib import gcs


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

