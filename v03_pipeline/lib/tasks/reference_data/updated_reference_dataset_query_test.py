import hail as hl
import luigi
import responses

from v03_pipeline.lib.misc.io import write
from v03_pipeline.lib.model.dataset_type import DatasetType
from v03_pipeline.lib.model.definitions import ReferenceGenome, SampleType
from v03_pipeline.lib.paths import valid_reference_dataset_path
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    ReferenceDataset,
    ReferenceDatasetQuery,
)
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset_query import (
    UpdatedReferenceDatasetQueryTask,
)
from v03_pipeline.lib.test.mock_clinvar_urls import mock_clinvar_urls
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase


class UpdatedReferenceDatasetCollectionTaskTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        # clinvar ReferenceDataset exists but is old
        # clinvar_path ReferenceDatasetQuery dne
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
                globals=hl.Struct(version='2021-01-01'),
            ),
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.clinvar,
            ),
        )

    @responses.activate
    def test_updated_query_and_dependency(
        self,
    ) -> None:
        with mock_clinvar_urls():
            worker = luigi.worker.Worker()
            task = UpdatedReferenceDatasetQueryTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SNV_INDEL,
                reference_dataset_query=ReferenceDatasetQuery.clinvar_path_variants,
                sample_type=SampleType.WGS,
                callset_path='',
                project_guids=[],
                project_remap_paths=[],
                project_pedigree_paths=[],
                skip_validation=True,
                run_id='1',
            )
            worker.add(task)
            worker.run()
            self.assertTrue(task.complete())
        clinvar_ht_path = valid_reference_dataset_path(
            ReferenceGenome.GRCh38,
            ReferenceDataset.clinvar,
        )
        clinvar_ht = hl.read_table(clinvar_ht_path)
        self.assertTrue('2024-11-11' in clinvar_ht_path)
        self.assertEqual(
            hl.eval(clinvar_ht.version),
            '2024-11-11',
        )
        self.assertTrue(hasattr(clinvar_ht, 'submitters'))
        clinvar_path_ht_path = valid_reference_dataset_path(
            ReferenceGenome.GRCh38,
            ReferenceDatasetQuery.clinvar_path,
        )
        clinvar_path_ht = hl.read_table(clinvar_path_ht_path)
        self.assertTrue('2024-11-11' in clinvar_path_ht_path)
        self.assertEqual(
            hl.eval(clinvar_path_ht.version),
            '2024-11-11',
        )
        self.assertTrue(hasattr(clinvar_path_ht, 'is_likely_pathogenic'))
