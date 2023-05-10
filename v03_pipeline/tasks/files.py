import os

import luigi
from luigi.contrib import gcs


def GCSorLocalTarget(filename: str) -> luigi.Target:  # noqa: N802
    return (
        gcs.GCSTarget(filename)
        if filename.startswith('gs://')
        else luigi.LocalTarget(filename)
    )


class VCFFile(luigi.Task):
    filename = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(self.filename)

    def complete(self) -> bool:
        # NB: hail supports reading glob bgz files.
        if '*' in self.filename:
            return GCSorLocalTarget(os.path.join(self.filename, '_SUCCESS')).exists()
        return GCSorLocalTarget(self.filename).exists()


class HailTable(luigi.Task):
    filename = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(self.filename)

    def complete(self) -> bool:
        return GCSorLocalTarget(os.path.join(self.filename, '_SUCCESS')).exists()
