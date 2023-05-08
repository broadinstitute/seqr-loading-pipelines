import os

import luigi
from luigi.contrib import gcs


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


class HailTable(luigi.Task):
    filename = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(self.filename)

    def complete(self) -> bool:
        # Complete is called by Luigi to check if the task is done and will skip if it is.
        # By default it checks to see that the output exists, but we want to check for the
        # _SUCCESS file to make sure it was not terminated halfway.
        return GCSorLocalTarget(os.path.join(self.filename, '_SUCCESS')).exists()
