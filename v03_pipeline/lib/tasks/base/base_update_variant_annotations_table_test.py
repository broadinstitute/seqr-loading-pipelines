import hail as hl
import luigi.worker
import responses

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import valid_reference_dataset_query_path
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDatasetQuery
from v03_pipeline.lib.tasks.base.base_update_variant_annotations_table import (
    BaseUpdateVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget
from v03_pipeline.lib.test.mock_clinvar_urls import mock_clinvar_urls
from v03_pipeline.lib.test.mocked_reference_datasets_testcase import (
    MockedReferenceDatasetsTestCase,
)


class BaseVariantAnnotationsTableTest(MockedReferenceDatasetsTestCase):
    @responses.activate
    def test_should_create_initialized_table(
        self,
    ) -> None:
        with mock_clinvar_urls(ReferenceGenome.GRCh38):
            vat_task = BaseUpdateVariantAnnotationsTableTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SNV_INDEL,
            )
            self.assertTrue('annotations.ht' in vat_task.output().path)
            self.assertFalse(vat_task.output().exists())
            self.assertFalse(vat_task.complete())

            worker = luigi.worker.Worker()
            worker.add(vat_task)
            worker.run()
            self.assertTrue(GCSorLocalFolderTarget(vat_task.output().path).exists())
            self.assertTrue(vat_task.complete())

            ht = hl.read_table(vat_task.output().path)
            self.assertEqual(ht.count(), 0)
            self.assertEqual(list(ht.key.keys()), ['locus', 'alleles'])
            self.assertEqual(
                hl.eval(
                    hl.read_table(
                        valid_reference_dataset_query_path(
                            ReferenceGenome.GRCh38,
                            DatasetType.SNV_INDEL,
                            ReferenceDatasetQuery.clinvar_path_variants,
                        ),
                    ).globals.version,
                ),
                '2024-11-11',
            )
