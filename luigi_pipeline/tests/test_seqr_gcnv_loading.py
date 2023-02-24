import shutil
import tempfile
import unittest
from unittest import mock

import hail as hl
import luigi.worker

from seqr_gcnv_loading import SeqrGCNVVariantMTTask, SeqrGCNVGenotypesMTTask, SeqrGCNVMTToESTask

NEW_JOINT_TSV_DATA = [
    'chr    start   end name    sample  sample_fix  svtype  GT  CN  NP  QA  QS  QSE QSS ploidy  strand  variant_name    ID  rmsstd  defragmented    vaf vac lt100_raw_calls lt10_highQS_rare_calls  PASS_SAMPLE PASS_FREQ   PASS_QS HIGH_QUALITY    genes_any_overlap   genes_any_overlap_exonsPerGene  genes_any_overlap_totalExons    genes_strict_overlap    genes_strict_overlap_exonsPerGene   genes_strict_overlap_totalExons genes_CG    genes_LOF   genes_any_overlap_Ensemble_ID   genes_LOF_Ensemble_ID   genes_CG_Ensemble_ID    var_source  callset_ovl identical_ovl   partial_0_5_ovl any_ovl no_ovl  strvctvre_score\n',
    'chr1   100006937   100007881   COHORT_1_Y_cnv_16326    RP-2037_PIE_OGI2271_003780_D1_v1_Exome_GCP  PIE_OGI2271_003780_D1_v1_Exome_RP-2037  DEL 1   1   1   4   4   4   4   2   -   suffix_16453    115884  0   FALSE   0.00004401408   1   TRUE    TRUE    TRUE    TRUE    FALSE   FALSE   AC118553.2,SLC35A3  1,1 2   None    0   0   NA  AC118553.2,SLC35A3  ENSG00000283761.1,ENSG00000117620.15    ENSG00000283761.1,ENSG00000117620.15        round2  round1  cluster_6_CASE_cnv_74577    cluster_6_CASE_cnv_74577    cluster_6_CASE_cnv_74577    FALSE   0.583\n',
    'chr1   100017585   100023213   COHORT_13_Y_cnv_13436   C1981_PIE_OGI313_000747_1_v2_Exome_GCP  PIE_OGI313_000747_1_v2_Exome_C1981  DEL 1   1   2   4   5   5   4   2   -   suffix_16456    115888  0   FALSE   0.00008802817   2   FALSE   FALSE   FALSE   TRUE    FALSE   FALSE   AC118553.2,SLC35A3  1,2 3   None    0   0   NA  AC118553.2,SLC35A3  ENSG00000283761.1,ENSG00000117620.15    ENSG00000283761.1,ENSG00000117620.15        round2  round1  cluster_9_CASE_cnv_52542    cluster_9_CASE_cnv_52542    cluster_9_CASE_cnv_52542    FALSE   0.507\n',
    'chr1   100017585   100023213   CASE_10_X_cnv_38690 C1992_MAN_0354_01_1_v1_Exome_GCP    MAN_0354_01_1_v1_Exome_C1992    DEL 1   0   2   20  30  30  20  2   -   suffix_16456    115887  0   FALSE   0.00008802817   2   FALSE   FALSE   FALSE   TRUE    FALSE   FALSE   AC118553.2,SLC35A3  1,2 3   None    0   0   NA  AC118553.2,SLC35A3  ENSG00000283761.1,ENSG00000117620.15    ENSG00000283761.1,ENSG00000117620.15        round2  round1  cluster_22_COHORT_cnv_56835 cluster_22_COHORT_cnv_56835 cluster_22_COHORT_cnv_56835 FALSE   0.507\n',
    'chr1   100022289   100023213   COHORT_13_Y_cnv_25376   C1990_GLE-4772-4-2-a_v1_Exome_GCP   GLE-4772-4-2-a_v1_Exome_C1990   DEL 1   1   1   3   3   3   3   2   -   suffix_16457    115889  0   FALSE   0.00004401408   1   TRUE    TRUE    TRUE    TRUE    FALSE   FALSE   SLC35A3 1   1   None    0   0   NA  SLC35A3 ENSG00000117620.15  ENSG00000117620.15      round2  NA              TRUE    0.502\n',
]

MERGED_TSV_DATA = [
    'chr    start   end name    sample  sample_fix  svtype  GT  CN  NP  QA  QS  QSE QSS ploidy  strand  variant_name    ID  rmsstd  defragmented    vaf vac lt100_raw_calls lt10_highQS_rare_calls  PASS_SAMPLE PASS_FREQ   PASS_QS HIGH_QUALITY    genes_any_overlap   genes_any_overlap_exonsPerGene  genes_any_overlap_totalExons    genes_strict_overlap    genes_strict_overlap_exonsPerGene   genes_strict_overlap_totalExons genes_CG    genes_LOF   genes_any_overlap_Ensemble_ID   genes_LOF_Ensemble_ID   genes_CG_Ensemble_ID    var_source  callset_ovl identical_ovl   partial_0_5_ovl any_ovl no_ovl  strvctvre_score is_latest\n',
    'chr1   100006937   100007881   COHORT_1_Y_cnv_16326    RP-2037_PIE_OGI2271_003780_D1_v1_Exome_GCP  PIE_OGI2271_003780_D1_v1_Exome_RP-2037  DEL 1   1   1   4   4   4   4   2   -   suffix_16453    115884  0   FALSE   4.401408e-05    1   TRUE    TRUE    TRUE    TRUE    FALSE   FALSE   AC118553.2,SLC35A3  1,1 2   None    0   0   NA  AC118553.2,SLC35A3  ENSG00000283761.1,ENSG00000117620.15    ENSG00000283761.1,ENSG00000117620.15        round2  round1  cluster_6_CASE_cnv_74577    cluster_6_CASE_cnv_74577    cluster_6_CASE_cnv_74577    FALSE   0.583   FALSE\n',
    'chr1   100017585   100023213   COHORT_13_Y_cnv_13436   C1981_PIE_OGI313_000747_1_v2_Exome_GCP  PIE_OGI313_000747_1_v2_Exome_C1981  DEL 1   1   2   4   5   5   4   2   -   suffix_16456    115888  0   FALSE   8.802817e-05    2   FALSE   FALSE   FALSE   TRUE    FALSE   FALSE   AC118553.2,SLC35A3  1,2 3   None    0   0   NA  AC118553.2,SLC35A3  ENSG00000283761.1,ENSG00000117620.15    ENSG00000283761.1,ENSG00000117620.15        round2  round1  cluster_9_CASE_cnv_52542    cluster_9_CASE_cnv_52542    cluster_9_CASE_cnv_52542    FALSE   0.507   TRUE\n',
    'chr1   100017585   100023213   CASE_10_X_cnv_38690 C1992_MAN_0354_01_1_v1_Exome_GCP    MAN_0354_01_1_v1_Exome_C1992    DEL 1   0   2   20  30  30  20  2   -   suffix_16456    115887  0   FALSE   8.802817e-05    2   FALSE   FALSE   FALSE   TRUE    FALSE   FALSE   AC118553.2,SLC35A3  1,2 3   None    0   0   NA  AC118553.2,SLC35A3  ENSG00000283761.1,ENSG00000117620.15    ENSG00000283761.1,ENSG00000117620.15        round2  round1  cluster_22_COHORT_cnv_56835 cluster_22_COHORT_cnv_56835 cluster_22_COHORT_cnv_56835 FALSE   0.507   FALSE\n',
]

class SeqrGCNVLoadingTest(unittest.TestCase):
    def setUp(self):
        self._temp_dir = tempfile.TemporaryDirectory()
        self._new_joint_bed_file = tempfile.mkstemp(dir=self._temp_dir.name, suffix='.tsv')[1]
        self._merged_bed_file = tempfile.mkstemp(dir=self._temp_dir.name, suffix='.tsv')[1]
        self._variant_mt_file = tempfile.mkstemp(dir=self._temp_dir.name, suffix='.mt')[1]
        self._genotypes_mt_file = tempfile.mkstemp(dir=self._temp_dir.name, suffix='.mt')[1]
        with open(self._new_joint_bed_file, 'w') as f:
            f.writelines('\n'.join(NEW_JOINT_TSV_DATA))
        with open(self._merged_bed_file, 'w') as f:
            f.writelines('\n'.join(MERGED_TSV_DATA))

    def tearDown(self):
        print("CALLED")
        shutil.rmtree(self._temp_dir.name)

    def test_run_new_joint_tsv_task(self):
        worker = luigi.worker.Worker()
        # Our framework doesn't pass the parameters to the dependent task.. so we force them
        # here.
        SeqrGCNVVariantMTTask.source_paths = self._new_joint_bed_file
        SeqrGCNVVariantMTTask.dest_path = self._variant_mt_file
        genotype_task = SeqrGCNVGenotypesMTTask(
            genome_version="38",
            source_paths="i am completely ignored",
            dest_path=self._genotypes_mt_file
        )
        worker.add(genotype_task)
        worker.run()

    def test_run_merged_tsv_task(self):
        worker = luigi.worker.Worker()
        # Our framework doesn't pass the parameters to the dependent task.. so we force them
        # here.
        SeqrGCNVVariantMTTask.source_paths = self._merged_bed_file
        SeqrGCNVVariantMTTask.dest_path = self._variant_mt_file
        genotype_task = SeqrGCNVGenotypesMTTask(
            genome_version="38",
            source_paths="i am completely ignored",
            dest_path=self._genotypes_mt_file
        )
        worker.add(genotype_task)
        worker.run()