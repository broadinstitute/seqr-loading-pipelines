import json
from unittest.mock import Mock, patch

import hail as hl
import hailtop.fs as hfs
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.write_sample_qc_json import WriteSampleQCJsonTask
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_TDR_METRICS_FILE = 'v03_pipeline/var/test/tdr_metrics.tsv'
TEST_RUN_ID = 'manual__2024-04-03'

PCA_SCORES = [0.00212, 0.011, 0.0105, 0.161, 0.026] + [0.1 for _ in range(15)]
EXPECTED_QC_POP = {
    'qc_pop': 'asj',
    'prob_afr': 0.02,
    'prob_ami': 0.0,
    'prob_amr': 0.02,
    'prob_asj': 0.9,
    'prob_eas': 0.0,
    'prob_fin': 0.0,
    'prob_mid': 0.0,
    'prob_nfe': 0.05,
    'prob_sas': 0.01,
}


class WriteSampleQCJsonTaskTest(MockedDatarootTestCase):
    @patch('v03_pipeline.lib.methods.sample_qc.assign_population_pcs')
    @patch('v03_pipeline.lib.methods.sample_qc._get_pop_pca_scores')
    @patch('v03_pipeline.lib.tasks.write_sample_qc_json.WriteTDRMetricsFilesTask')
    @patch('v03_pipeline.lib.tasks.write_sample_qc_json.hfs.ls')
    def test_call_sample_qc(
        self,
        mock_ls_tdr_dir: Mock,
        mock_tdr_task: Mock,
        mock_get_pop_pca_scores: Mock,
        mock_assign_population_pcs: Mock,
    ) -> None:
        mock_tdr_task.return_value = MockCompleteTask()
        mock_tdr_table = Mock()
        mock_tdr_table.path = TEST_TDR_METRICS_FILE
        mock_ls_tdr_dir.return_value = [mock_tdr_table]
        mock_get_pop_pca_scores.return_value = hl.Table.parallelize(
            [
                {
                    's': sample_id,
                    'scores': PCA_SCORES,
                    'known_pop': 'Unknown',
                }
                for sample_id in ('HG00731', 'HG00732', 'HG00733', 'NA19675')
            ],
            hl.tstruct(
                s=hl.tstr,
                scores=hl.tarray(hl.tfloat64),
                known_pop=hl.tstr,
            ),
            key='s',
        )
        mock_assign_population_pcs.return_value = (
            hl.Table.parallelize(
                [
                    {
                        's': sample_id,
                        'pca_scores': PCA_SCORES,
                        **EXPECTED_QC_POP,
                    }
                    for sample_id in ('HG00731', 'HG00732', 'HG00733', 'NA19675')
                ],
                hl.tstruct(
                    s=hl.tstr,
                    pca_scores=hl.tarray(hl.tfloat64),
                    qc_pop=hl.tstr,
                    prob_afr=hl.tfloat64,
                    prob_ami=hl.tfloat64,
                    prob_amr=hl.tfloat64,
                    prob_asj=hl.tfloat64,
                    prob_eas=hl.tfloat64,
                    prob_fin=hl.tfloat64,
                    prob_mid=hl.tfloat64,
                    prob_nfe=hl.tfloat64,
                    prob_sas=hl.tfloat64,
                ),
                key='s',
            ),
            None,
        )

        worker = luigi.worker.Worker()
        task = WriteSampleQCJsonTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id=TEST_RUN_ID,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_guids=['R0113_test_project'],
            skip_validation=True,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task)
        self.assertTrue(hfs.exists(task.output().path))

        expected_qc_pop_results = {
            'pca_scores': PCA_SCORES,
            **EXPECTED_QC_POP,
            **{f'pop_PC{i + 1}': PCA_SCORES[i] for i in range(20)},
        }
        with task.output().open('r') as f:
            res = json.load(f)

        self.assertCountEqual(
            res['HG00731'],
            {
                'filtered_callrate': 1.0,
                'contamination_rate': 5.099999904632568,
                'percent_bases_at_20x': 93.69000244140625,
                'mean_coverage': 29.309999465942383,
                'filter_flags': ['contamination', 'coverage'],
                **expected_qc_pop_results,
            },
        )
        self.assertCountEqual(
            res['HG00732'],
            {
                'filtered_callrate': 1.0,
                'contamination_rate': 5.0,
                'percent_bases_at_20x': 90.0,
                'mean_coverage': 28.0,
                'filter_flags': ['coverage'],
                **expected_qc_pop_results,
            },
        )
        self.assertCountEqual(
            res['HG00733'],
            {
                'filtered_callrate': 1.0,
                'contamination_rate': 6.0,
                'percent_bases_at_20x': 85.0,
                'mean_coverage': 36.400001525878906,
                'filter_flags': ['contamination'],
                **expected_qc_pop_results,
            },
        )
        self.assertCountEqual(
            res['NA19675'],
            {
                'filtered_callrate': 1.0,
                'contamination_rate': 0.0,
                'percent_bases_at_20x': 80.0,
                'mean_coverage': 30.0,
                'filter_flags': [],
                **expected_qc_pop_results,
            },
        )
