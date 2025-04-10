import unittest

import hail as hl

from v03_pipeline.lib.annotations.misc import (
    unmap_formatting_annotation_enums,
)
from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
)

TEST_SNV_INDEL_ANNOTATIONS = (
    'v03_pipeline/var/test/exports/GRCh38/SNV_INDEL/annotations.ht'
)


class MiscTest(unittest.TestCase):
    def unmap_formatting_annotation_enums(self) -> None:
        ht = hl.read_table(TEST_SNV_INDEL_ANNOTATIONS)
        ht = unmap_formatting_annotation_enums(
            ht, ReferenceGenome.GRCh38, DatasetType.SNV_INDEL,
        )
