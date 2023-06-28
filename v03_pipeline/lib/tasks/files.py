import os

import luigi
from luigi.contrib import gcs

GLOB = '*'


def CallsetTask(pathname: str) -> luigi.Task:  # noqa: N802
    if 'vcf' in pathname:
        return VCFFileTask(pathname)
    if pathname.endswith('mt'):
        return HailTableTask(pathname)
    return RawFileTask(pathname)


def GCSorLocalTarget(pathname: str) -> luigi.Target:  # noqa: N802
    return (
        gcs.GCSTarget(pathname)
        if pathname.startswith('gs://')
        else luigi.LocalTarget(pathname)
    )


def GCSorLocalFolderTarget(pathname: str) -> luigi.Target:  # noqa: N802
    return GCSorLocalTarget(os.path.join(pathname, '_SUCCESS'))


class RawFileTask(luigi.Task):
    pathname = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(self.pathname)


class VCFFileTask(RawFileTask):
    def complete(self) -> bool:
        # NB: hail supports reading glob bgz files.
        if GLOB in self.pathname:
            return GCSorLocalTarget(os.path.dirname(self.pathname)).exists()
        return GCSorLocalTarget(self.pathname).exists()


class HailTableTask(RawFileTask):
    def complete(self) -> bool:
        return GCSorLocalFolderTarget(self.pathname).exists()
