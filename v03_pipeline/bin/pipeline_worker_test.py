import json
import os
from unittest.mock import Mock, patch

import hail as hl
import hailtop.fs as hfs
import luigi
import luigi.worker

from v03_pipeline.bin.pipeline_worker import process_queue
from v03_pipeline.lib.test.clickhouse_schema_testcase import ClickhouseSchemaTestCase
from v03_pipeline.lib.core import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.core.environment import Env
from v03_pipeline.lib.misc.clickhouse import (
    STAGING_CLICKHOUSE_DATABASE,
    ClickhouseReferenceDataset,
    get_clickhouse_client,
)
from v03_pipeline.lib.paths import (
    clickhouse_load_success_file_path,
    loading_pipeline_deadletter_queue_dir,
    loading_pipeline_queue_dir,
)
from v03_pipeline.lib.test.misc import copy_project_pedigree_to_mocked_dir
from v03_pipeline.lib.test.mocked_reference_datasets_testcase import (
    MockedReferenceDatasetsTestCase,
)
from v03_pipeline.var.test.vep.mock_vep_data import MOCK_38_VEP_DATA

TEST_PEDIGREE_3_REMAP = 'v03_pipeline/var/test/pedigrees/test_pedigree_3_remap.tsv'
TEST_SCHEMA = 'v03_pipeline/var/test/test_clickhouse_schema.sql'
TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'


class MyFailingTask(luigi.Task):
    def run(self):
        msg = 'This task is designed to fail!'
        raise ValueError(msg)

    def output(self):
        return luigi.LocalTarget('output.txt')


class PipelineWorkerTest(MockedReferenceDatasetsTestCase, ClickhouseSchemaTestCase):
    @patch.object(
        ClickhouseReferenceDataset,
        'for_reference_genome_dataset_type',
        return_value=[ClickhouseReferenceDataset.CLINVAR],
    )
    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.load_gencode_ensembl_to_refseq_id',
    )
    @patch(
        'v03_pipeline.lib.tasks.update_new_variants_with_caids.register_alleles_in_chunks',
    )
    @patch('v03_pipeline.lib.vep.hl.vep')
    @patch('v03_pipeline.lib.misc.slack._safe_post_to_slack')
    @patch('v03_pipeline.bin.pipeline_worker.logger')
    def test_process_queue_integration_test(
        self,
        mock_logger,
        mock_safe_post_to_slack,
        mock_vep: Mock,
        mock_register_alleles: Mock,
        mock_load_gencode_ensembl_to_refseq_id: Mock,
        mock_for_reference_genome_dataset_type: Mock,
    ):
        mock_load_gencode_ensembl_to_refseq_id.return_value = hl.dict(
            {'ENST00000327044': 'NM_015658.4'},
        )
        mock_register_alleles.side_effect = None
        mock_vep.side_effect = lambda ht, **_: ht.annotate(vep=MOCK_38_VEP_DATA)
        copy_project_pedigree_to_mocked_dir(
            TEST_PEDIGREE_3_REMAP,
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            SampleType.WGS,
            'R0113_test_project',
        )
        raw_request = {
            'request_type': 'LoadingPipelineRequest',
            'callset_path': TEST_VCF,
            'projects_to_run': ['R0113_test_project'],
            'sample_type': SampleType.WGS.value,
            'reference_genome': ReferenceGenome.GRCh38.value,
            'dataset_type': DatasetType.SNV_INDEL.value,
            'validations_to_skip': ['all'],
        }
        os.makedirs(
            loading_pipeline_queue_dir(),
            exist_ok=True,
        )
        with open(
            os.path.join(
                loading_pipeline_queue_dir(),
                'request_20250916-200704-123456.json',
            ),
            'w',
        ) as f:
            json.dump(raw_request, f)
        process_queue(local_scheduler=True)
        mock_safe_post_to_slack.assert_called_once_with(
            ':white_check_mark: Pipeline Runner Request Success! :white_check_mark:\nRun ID: 20250916-200704-123456\n```{\n    "attempt_id": 0,\n    "callset_path": "v03_pipeline/var/test/callsets/1kg_30variants.vcf",\n    "dataset_type": "SNV_INDEL",\n    "project_guids": [\n        "R0113_test_project"\n    ],\n    "reference_genome": "GRCh38",\n    "request_type": "LoadingPipelineRequest",\n    "sample_type": "WGS",\n    "skip_check_sex_and_relatedness": false,\n    "skip_expect_tdr_metrics": false,\n    "validations_to_skip": [\n        "validate_allele_depth_length",\n        "validate_allele_type",\n        "validate_expected_contig_frequency",\n        "validate_no_duplicate_variants",\n        "validate_sample_type"\n    ]\n}```',
        )
        with hfs.open(
            clickhouse_load_success_file_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                '20250916-200704-123456',
            ),
        ) as f:
            self.assertEqual(f.read(), '')

        client = get_clickhouse_client()
        annotations_count = client.execute(
            f"""
            SELECT COUNT(*)
            FROM
            {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/annotations_memory`
            """,
        )[0][0]
        self.assertEqual(annotations_count, 30)
        entries_count = client.execute(
            f"""
            SELECT COUNT(*)
            FROM
            {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/entries`
            """,
        )[0][0]
        self.assertEqual(entries_count, 16)
        ac_wgs = client.execute(
            f"""
            SELECT sum(ac_wgs)
            FROM
            {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/gt_stats_dict`
            """,
        )[0][0]
        self.assertEqual(ac_wgs, 69)

    @patch('v03_pipeline.lib.misc.slack._safe_post_to_slack')
    @patch('v03_pipeline.api.request_handlers.WriteClickhouseLoadSuccessFileTask')
    @patch('v03_pipeline.bin.pipeline_worker.logger')
    def test_process_failure(
        self,
        mock_logger,
        mock_write_clickhouse_load_success_file_task,
        mock_safe_post_to_slack,
    ):
        raw_request = {
            'request_type': 'LoadingPipelineRequest',
            'callset_path': TEST_VCF,
            'projects_to_run': ['project_a'],
            'sample_type': SampleType.WGS.value,
            'reference_genome': ReferenceGenome.GRCh38.value,
            'dataset_type': DatasetType.SNV_INDEL.value,
        }
        mock_write_clickhouse_load_success_file_task.return_value = MyFailingTask()
        os.makedirs(
            loading_pipeline_queue_dir(),
            exist_ok=True,
        )
        with open(
            os.path.join(
                loading_pipeline_queue_dir(),
                'request_20250918-200704-123456.json',
            ),
            'w',
        ) as f:
            json.dump(raw_request, f)
        process_queue(local_scheduler=True)
        process_queue(local_scheduler=True)
        process_queue(local_scheduler=True)
        process_queue(local_scheduler=True)
        process_queue(local_scheduler=True)
        mock_safe_post_to_slack.assert_called_once_with(
            ':failed: Pipeline Runner Request Failed :failed:\nRun ID: 20250918-200704-123456\n```{\n    "attempt_id": 4,\n    "callset_path": "v03_pipeline/var/test/callsets/1kg_30variants.vcf",\n    "dataset_type": "SNV_INDEL",\n    "project_guids": [\n        "project_a"\n    ],\n    "reference_genome": "GRCh38",\n    "request_type": "LoadingPipelineRequest",\n    "sample_type": "WGS",\n    "skip_check_sex_and_relatedness": false,\n    "skip_expect_tdr_metrics": false,\n    "validations_to_skip": []\n}```\nReason: there were failed tasks',
        )
        self.assertEqual(len(os.listdir(loading_pipeline_queue_dir())), 0)
        with open(
            os.path.join(
                loading_pipeline_deadletter_queue_dir(),
                'request_20250918-200704-123456.json',
            ),
        ) as f:
            r = json.load(f)
            self.assertEqual(r['request_type'], 'LoadingPipelineRequest')
            self.assertEqual(r['attempt_id'], 4)
