from unittest.mock import patch

import hail as hl
import luigi.worker

from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.paths import valid_reference_dataset_path
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    BaseReferenceDataset,
    ReferenceDataset,
)
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget
from v03_pipeline.lib.tasks.reference_data.update_variant_annotations_table_with_updated_reference_dataset import (
    UpdateVariantAnnotationsTableWithUpdatedReferenceDataset,
)
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_SNV_INDEL_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'


class UpdateVATWithUpdatedRDC(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        # TODO replace mocked reference datasets with more representative ones
        write(
            hl.Table.parallelize(
                [
                    {
                        'locus': hl.Locus(
                            contig='chr1',
                            position=1,
                            reference_genome='GRCh38',
                        ),
                        'alleles': ['A', 'C'],
                    },
                ],
                hl.tstruct(
                    locus=hl.tlocus('GRCh38'),
                    alleles=hl.tarray(hl.tstr),
                ),
                key=['locus', 'alleles'],
                globals=hl.Struct(
                    version=ReferenceDataset.clinvar.version(ReferenceGenome.GRCh38),
                    enums=ReferenceDataset.clinvar.enum_globals,
                ),
            ),
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.clinvar,
            ),
        )

    @patch(
        'v03_pipeline.lib.tasks.base.base_update_variant_annotations_table.UpdatedReferenceDatasetTask',
    )
    @patch(
        'v03_pipeline.lib.tasks.base.base_update_variant_annotations_table.UpdatedReferenceDatasetQueryTask',
    )
    def test_create_annotations_table(
        self,
        mock_rd_query_task,
        mock_rd_task,
    ):
        mock_rd_task.return_value = MockCompleteTask()
        mock_rd_query_task.return_value = MockCompleteTask()

        # Start testing with a mocked clinvar table
        with patch.object(
            BaseReferenceDataset,
            'for_reference_genome_dataset_type',
            return_value=[ReferenceDataset.clinvar],
        ):
            task = UpdateVariantAnnotationsTableWithUpdatedReferenceDataset(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SNV_INDEL,
                sample_type=SampleType.WGS,
                callset_path=TEST_SNV_INDEL_VCF,
                project_guids=[],
                project_remap_paths=[],
                project_pedigree_paths=[],
                skip_validation=True,
                run_id='3',
            )
            worker = luigi.worker.Worker()
            worker.add(task)
            worker.run()
            self.assertTrue(GCSorLocalFolderTarget(task.output().path).exists())
            self.assertTrue(task.complete())

            ht = hl.read_table(task.output().path)
            self.assertCountEqual(
                ht.globals.collect()[0],
                hl.Struct(
                    versions=hl.Struct(
                        clinvar=ReferenceDataset.clinvar.version(
                            ReferenceGenome.GRCh38,
                        ),
                    ),
                    enums=hl.Struct(clinvar=ReferenceDataset.clinvar.enum_globals),
                    migrations=[],
                    updates=set(),
                ),
            )
