import os

import luigi
from luigi.contrib import gcs

BGZ_GLOB_SUFFIX = '/*.bgz'


def GCSorLocalTarget(pathname: str) -> luigi.Target:  # noqa: N802
    return (
        gcs.GCSTarget(pathname)
        if pathname.startswith('gs://')
        else luigi.LocalTarget(pathname)
    )


def GCSorLocalFolderTarget(pathname: str) -> luigi.Target:  # noqa: N802
    return GCSorLocalTarget(os.path.join(pathname, '_SUCCESS'))


class RawFile(luigi.Task):
    pathname = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(self.pathname)


class VCFFile(RawFile):
    def complete(self) -> bool:
        # NB: hail supports reading glob bgz files.
        if self.pathname.endswith(BGZ_GLOB_SUFFIX):
            glob_stripped = self.pathname.replace(BGZ_GLOB_SUFFIX, '/')
            return GCSorLocalFolderTarget(glob_stripped).exists()
        return GCSorLocalTarget(self.pathname).exists()


class HailTable(RawFile):
    def complete(self) -> bool:
        return GCSorLocalFolderTarget(self.pathname, '_SUCCESS').exists()
