import unittest

import luigi

from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.tasks.reference_data.write_cached_reference_dataset_query import (
    WriteCachedReferenceDatasetQuery,
)


class WriteCachedReferenceDatasetQueryTest(unittest.TestCase):
    def test_37_snv_indel(self):
        worker = luigi.worker.Worker()
        task = WriteCachedReferenceDatasetQuery(
            reference_genome=ReferenceGenome.GRCh37,
            dataset_type=DatasetType.SNV_INDEL,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())

    def test_38_snv_indel(self):
        pass

    def test_38_mito(self):
        pass

    def test_38_ont_snv_indel(self):
        pass
