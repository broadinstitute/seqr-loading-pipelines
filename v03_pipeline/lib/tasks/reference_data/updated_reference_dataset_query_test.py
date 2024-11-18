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
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

CLINVAR_VCF = 'v03_pipeline/var/test/reference_data/clinvar.vcf.gz'


class UpdatedReferenceDatasetCollectionTaskTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
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
        with open(CLINVAR_VCF, 'rb') as f:
            responses.add_passthru('http://localhost')
            responses.get(
                ReferenceDataset.clinvar.raw_dataset_path(ReferenceGenome.GRCh38),
                body=f.read(),
            )
            worker = luigi.worker.Worker()
            task = UpdatedReferenceDatasetQueryTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SNV_INDEL,
                reference_dataset_query=ReferenceDatasetQuery.clinvar_path,
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
