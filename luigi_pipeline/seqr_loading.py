import luigi

from lib.hail_tasks import HailMatrixTableTask, HailElasticSearchTask, GCSorLocalTarget


class SeqrVCFToMTTask(HailMatrixTableTask):
    """
    Inherits from a Hail MT Class to get helper function logic. Main logic to do annotations here.
    """
    reference_mt_path = luigi.Parameter(description='Path to the matrix table storing the reference variants.')

    def run(self):
        mt = self.import_vcf()
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
