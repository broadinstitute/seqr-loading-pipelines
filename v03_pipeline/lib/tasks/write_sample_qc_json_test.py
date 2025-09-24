import json
import os
import shutil
from unittest.mock import Mock, patch

import hail as hl
import hailtop.fs as hfs
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.paths import ancestry_model_rf_path
from v03_pipeline.lib.tasks.write_sample_qc_json import WriteSampleQCJsonTask
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_reference_datasets_testcase import (
    MockedReferenceDatasetsTestCase,
)

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_TDR_METRICS_FILE = 'v03_pipeline/var/test/tdr_metrics.tsv'
TEST_RUN_ID = 'manual__2024-04-03'

PCA_SCORES = [0.00212, 0.011, 0.0105, 0.161, 0.026] + [0.1 for _ in range(15)]
EXPECTED_ANC_PROBABILITIES = {
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
TEST_ANCESTRY_IMPUTATION_MODEL_PATH = (
    'v03_pipeline/var/test/ancestry_imputation_model.onnx'
)


class WriteSampleQCJsonTaskTest(MockedReferenceDatasetsTestCase):
    @patch('v03_pipeline.lib.methods.sample_qc.assign_population_pcs')
    @patch('v03_pipeline.lib.methods.sample_qc.pc_project')
    @patch('v03_pipeline.lib.tasks.write_sample_qc_json.WriteTDRMetricsFilesTask')
    @patch('v03_pipeline.lib.tasks.write_sample_qc_json.hfs.ls')
    def test_call_sample_qc(
        self,
        mock_ls_tdr_dir: Mock,
        mock_tdr_task: Mock,
        mock_pc_project: Mock,
        mock_assign_population_pcs: Mock,
    ) -> None:
        os.makedirs(
            os.path.dirname(ancestry_model_rf_path()),
            exist_ok=True,
        )
        shutil.copy2(TEST_ANCESTRY_IMPUTATION_MODEL_PATH, ancestry_model_rf_path())
        mock_tdr_task.return_value = MockCompleteTask()
        mock_tdr_table = Mock()
        mock_tdr_table.path = TEST_TDR_METRICS_FILE
        # Note, hfs.ls is getting mocked in every module not just write_sample_qc_json?
        # It seems as though hfs is import-cached somehow, which leads to a single
        # object being used globally.
        mock_tdr_table.size = 10
        mock_ls_tdr_dir.return_value = [mock_tdr_table]
        mock_pc_project.return_value = hl.Table.parallelize(
            [
                {
                    's': sample_id,
                    'scores': PCA_SCORES,
                }
                for sample_id in ('HG00731', 'HG00732', 'HG00733', 'NA19675')
            ],
            hl.tstruct(
                s=hl.tstr,
                scores=hl.tarray(hl.tfloat64),
            ),
            key='s',
        )
        mock_assign_population_pcs.return_value = (
            hl.Table.parallelize(
                [
                    {
                        's': sample_id,
                        'pca_scores': PCA_SCORES,
                        'qc_pop': 'asj',
                        **EXPECTED_ANC_PROBABILITIES,
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
            validations_to_skip=['all'],
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task)
        self.assertTrue(hfs.exists(task.output().path))

        expected_qc_gen_anc_results = {
            'pca_scores': PCA_SCORES,
            **EXPECTED_ANC_PROBABILITIES,
            **{f'pop_PC{i + 1}': PCA_SCORES[i] for i in range(20)},
            'qc_gen_anc': 'oth',
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
                **expected_qc_gen_anc_results,
                'sample_qc.call_rate': 0.9666666666666667,
                'sample_qc.n_called': 29,
                'sample_qc.n_not_called': 1,
                'sample_qc.n_filtered': 0,
                'sample_qc.n_hom_ref': 16,
                'sample_qc.n_het': 2,
                'sample_qc.n_hom_var': 11,
                'sample_qc.n_non_ref': 13,
                'sample_qc.n_singleton': 1,
                'sample_qc.n_snp': 24,
                'sample_qc.n_insertion': 0,
                'sample_qc.n_deletion': 0,
                'sample_qc.n_transition': 15,
                'sample_qc.n_transversion': 9,
                'sample_qc.n_star': 0,
                'sample_qc.r_ti_tv': 1.6666666666666667,
                'sample_qc.r_het_hom_var': 0.18181818181818182,
                'sample_qc.r_insertion_deletion': None,
                'sample_qc.f_inbreeding.f_stat': 0.284195245767986,
                'sample_qc.f_inbreeding.n_called': 29,
                'sample_qc.f_inbreeding.expected_homs': 26.20594199999999,
                'sample_qc.f_inbreeding.observed_homs': 27,
                'fail_n_snp': True,
                'fail_r_ti_tv': False,
                'fail_r_insertion_deletion': None,
                'fail_n_insertion': True,
                'fail_n_deletion': True,
                'fail_r_het_hom_var': False,
                'fail_call_rate': False,
                'qc_metrics_filters': ['n_deletion', 'n_insertion', 'n_snp'],
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
                **expected_qc_gen_anc_results,
                'sample_qc.call_rate': 0.9666666666666667,
                'sample_qc.n_called': 29,
                'sample_qc.n_not_called': 1,
                'sample_qc.n_filtered': 0,
                'sample_qc.n_hom_ref': 16,
                'sample_qc.n_het': 4,
                'sample_qc.n_hom_var': 9,
                'sample_qc.n_non_ref': 13,
                'sample_qc.n_singleton': 2,
                'sample_qc.n_snp': 22,
                'sample_qc.n_insertion': 0,
                'sample_qc.n_deletion': 0,
                'sample_qc.n_transition': 13,
                'sample_qc.n_transversion': 9,
                'sample_qc.n_star': 0,
                'sample_qc.r_ti_tv': 1.4444444444444444,
                'sample_qc.r_het_hom_var': 0.4444444444444444,
                'sample_qc.r_insertion_deletion': None,
                'sample_qc.f_inbreeding.f_stat': -0.431609508464028,
                'sample_qc.f_inbreeding.n_called': 29,
                'sample_qc.f_inbreeding.expected_homs': 26.20594199999999,
                'sample_qc.f_inbreeding.observed_homs': 25,
                'fail_n_snp': True,
                'fail_r_ti_tv': False,
                'fail_r_insertion_deletion': None,
                'fail_n_insertion': True,
                'fail_n_deletion': True,
                'fail_r_het_hom_var': False,
                'fail_call_rate': False,
                'qc_metrics_filters': ['n_deletion', 'n_insertion', 'n_snp'],
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
                **expected_qc_gen_anc_results,
                'sample_qc.call_rate': 1.0,
                'sample_qc.n_called': 30,
                'sample_qc.n_not_called': 0,
                'sample_qc.n_filtered': 0,
                'sample_qc.n_hom_ref': 17,
                'sample_qc.n_het': 3,
                'sample_qc.n_hom_var': 10,
                'sample_qc.n_non_ref': 13,
                'sample_qc.n_singleton': 0,
                'sample_qc.n_snp': 23,
                'sample_qc.n_insertion': 0,
                'sample_qc.n_deletion': 0,
                'sample_qc.n_transition': 13,
                'sample_qc.n_transversion': 10,
                'sample_qc.n_star': 0,
                'sample_qc.r_ti_tv': 1.3,
                'sample_qc.r_het_hom_var': 0.3,
                'sample_qc.r_insertion_deletion': None,
                'sample_qc.f_inbreeding.f_stat': -0.038400752079048056,
                'sample_qc.f_inbreeding.n_called': 30,
                'sample_qc.f_inbreeding.expected_homs': 27.11094199999999,
                'sample_qc.f_inbreeding.observed_homs': 27,
                'fail_n_snp': True,
                'fail_r_ti_tv': False,
                'fail_r_insertion_deletion': None,
                'fail_n_insertion': True,
                'fail_n_deletion': True,
                'fail_r_het_hom_var': False,
                'fail_call_rate': False,
                'qc_metrics_filters': ['n_deletion', 'n_insertion', 'n_snp'],
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
                **expected_qc_gen_anc_results,
                'sample_qc.call_rate': 0.9666666666666667,
                'sample_qc.n_called': 29,
                'sample_qc.n_not_called': 1,
                'sample_qc.n_filtered': 0,
                'sample_qc.n_hom_ref': 18,
                'sample_qc.n_het': 1,
                'sample_qc.n_hom_var': 10,
                'sample_qc.n_non_ref': 11,
                'sample_qc.n_singleton': 1,
                'sample_qc.n_snp': 21,
                'sample_qc.n_insertion': 0,
                'sample_qc.n_deletion': 0,
                'sample_qc.n_transition': 12,
                'sample_qc.n_transversion': 9,
                'sample_qc.n_star': 0,
                'sample_qc.r_ti_tv': 1.3333333333333333,
                'sample_qc.r_het_hom_var': 0.1,
                'sample_qc.r_insertion_deletion': None,
                'sample_qc.f_inbreeding.f_stat': 0.6538664159736507,
                'sample_qc.f_inbreeding.n_called': 29,
                'sample_qc.f_inbreeding.expected_homs': 26.11094199999999,
                'sample_qc.f_inbreeding.observed_homs': 28,
                'fail_n_snp': False,
                'fail_r_ti_tv': False,
                'fail_r_insertion_deletion': None,
                'fail_n_insertion': True,
                'fail_n_deletion': True,
                'fail_r_het_hom_var': False,
                'fail_call_rate': True,
                'qc_metrics_filters': ['call_rate', 'n_deletion', 'n_insertion'],
            },
        )
