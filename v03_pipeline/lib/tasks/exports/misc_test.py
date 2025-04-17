import unittest

import hail as hl

from v03_pipeline.lib.annotations.misc import (
    unmap_formatting_annotation_enums,
    unmap_reference_dataset_annotation_enums,
)
from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
)
from v03_pipeline.lib.tasks.exports.misc import camelcase_array_structexpression_fields

TEST_SNV_INDEL_ANNOTATIONS = (
    'v03_pipeline/var/test/exports/GRCh38/SNV_INDEL/annotations.ht'
)


class MiscTest(unittest.TestCase):
    def test_camelcase_array_structexpression_fields(self) -> None:
        ht = hl.read_table(TEST_SNV_INDEL_ANNOTATIONS)
        ht = unmap_formatting_annotation_enums(
            ht, ReferenceGenome.GRCh38, DatasetType.SNV_INDEL,
        )
        ht = unmap_reference_dataset_annotation_enums(
            ht, ReferenceGenome.GRCh38, DatasetType.SNV_INDEL,
        )
        ht = camelcase_array_structexpression_fields(ht, ReferenceGenome.GRCh38)
