from unittest import mock

import hail as hl
import luigi.worker
import requests

from v03_pipeline.lib.misc.io import remap_pedigree_hash
from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.paths import variant_annotations_table_path
from v03_pipeline.lib.tasks.trigger_hail_backend_reload import TriggerHailBackendReload
from v03_pipeline.lib.test.misc import copy_project_pedigree_to_mocked_dir
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_PEDIGREE_3_REMAP = 'v03_pipeline/var/test/pedigrees/test_pedigree_3_remap.tsv'


class TriggerHailBackendReloadTestCase(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        annotations_ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                locus=hl.tlocus(ReferenceGenome.GRCh38.value),
                alleles=hl.tarray(hl.tstr),
            ),
            key=('locus', 'alleles'),
            globals=hl.Struct(
                paths=hl.Struct(),
                versions=hl.Struct(),
                enums=hl.Struct(),
                updates={
                    hl.Struct(
                        callset=TEST_VCF,
                        project_guid='R0113_test_project',
                        remap_pedigree_hash=hl.eval(
                            remap_pedigree_hash(TEST_PEDIGREE_3_REMAP),
                        ),
                    ),
                },
                migrations=hl.empty_array(hl.tstr),
            ),
        )
        annotations_ht.write(
            variant_annotations_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
            ),
        )
        copy_project_pedigree_to_mocked_dir(
            TEST_PEDIGREE_3_REMAP,
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            SampleType.WES,
            'R0113_test_project',
        )

    @mock.patch.object(requests, 'post')
    @mock.patch(
        'v03_pipeline.lib.tasks.trigger_hail_backend_reload.WriteSuccessFileTask',
    )
    def test_success(
        self,
        mock_write_success_file_task: mock.Mock,
        mock_post: mock.Mock,
    ):
        mock_write_success_file_task.return_value = MockCompleteTask()
        mock_resp = requests.models.Response()
        mock_resp.status_code = 200
        mock_post.return_value = mock_resp

        worker = luigi.worker.Worker()
        task = TriggerHailBackendReload(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WES,
            callset_path=TEST_VCF,
            project_guids=['R0113_test_project'],
            run_id='manual__2024-09-20',
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())

    @mock.patch.object(requests, 'post')
    @mock.patch(
        'v03_pipeline.lib.tasks.trigger_hail_backend_reload.WriteSuccessFileTask',
    )
    def test_failure(
        self,
        mock_write_success_file_task: mock.Mock,
        mock_post: mock.Mock,
    ):
        mock_write_success_file_task.return_value = MockCompleteTask()
        mock_resp = requests.models.Response()
        mock_resp.status_code = 500
        mock_post.return_value = mock_resp

        worker = luigi.worker.Worker()
        task = TriggerHailBackendReload(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WES,
            callset_path=TEST_VCF,
            project_guids=['R0113_test_project'],
            run_id='manual__2024-09-20',
        )
        worker.add(task)
        self.assertFalse(task.complete())
