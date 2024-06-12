import pathlib

import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.paths import family_table_path
from v03_pipeline.lib.tasks.delete_family_table import DeleteFamilyTableTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase


class DeleteTableTaskTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=876499,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['A', 'G'],
                },
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=878314,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['G', 'C'],
                },
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
            ),
            key=['locus', 'alleles'],
        )
        ht.write(
            family_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                'abc_1',
            ),
        )

    def test_delete_family_table_task(self) -> None:
        worker = luigi.worker.Worker()
        task = DeleteFamilyTableTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            family_guid='abc_1',
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
        self.assertFalse(
            pathlib.Path(
                family_table_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    'abc_1',
                ),
            ).exists(),
        )
