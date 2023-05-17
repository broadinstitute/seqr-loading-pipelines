import os
import tempfile
import unittest

import luigi.worker

from v03_pipeline.lib.definitions import Env, ReferenceGenome, DatasetType, SampleType
from v03_pipeline.lib.tasks.base_variant_annotations_table import BaseVariantAnnotationsTableTask


class BaseVariantAnnotationsTableTest(unittest.TestCase):
    
    def test_base_variant_annotations_table(self) -> None:
        worker = luigi.worker.Worker()
        variant_annotations = BaseVariantAnnotationsTableTask(
            env=Env.LOCAL,
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SV,
            sample_type=SampleType.WGS,
        )
        worker.add(variant_annotations)
        worker.run()