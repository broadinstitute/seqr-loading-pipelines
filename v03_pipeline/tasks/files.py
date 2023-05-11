import os

import luigi
from luigi.contrib import gcs

BGZ_GLOB_SUFFIX = '*.bgz'


def GCSorLocalTarget(filename: str) -> luigi.Target:  # noqa: N802
    return (
        gcs.GCSTarget(filename)
        if filename.startswith('gs://')
        else luigi.LocalTarget(filename)
    )


class RawFile(luigi.Task):
    filename = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(self.filename)


class VCFFile(RawFile):
    def complete(self) -> bool:
        # NB: hail supports reading glob bgz files.
        if self.filename.endswith(BGZ_GLOB_SUFFIX):
            glob_stripped = self.filename.replace(BGZ_GLOB_SUFFIX, '')
            return GCSorLocalTarget(os.path.join(glob_stripped, '_SUCCESS')).exists()
        return GCSorLocalTarget(self.filename).exists()


class HailTable(RawFile):
    def complete(self) -> bool:
        return GCSorLocalTarget(os.path.join(self.filename, '_SUCCESS')).exists()
