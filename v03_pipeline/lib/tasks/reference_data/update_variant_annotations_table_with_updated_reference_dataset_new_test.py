import shutil
from unittest.mock import patch

import hail as hl
import luigi.worker
import responses

from v03_pipeline.lib.annotations.enums import (
    BIOTYPES,
    FIVEUTR_CONSEQUENCES,
    LOF_FILTERS,
    MITOTIP_PATHOGENICITIES,
    MOTIF_CONSEQUENCE_TERMS,
    REGULATORY_BIOTYPES,
    REGULATORY_CONSEQUENCE_TERMS,
    TRANSCRIPT_CONSEQUENCE_TERMS,
)
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
from v03_pipeline.lib.test.mock_clinvar_urls import mock_clinvar_urls
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_DBNSFP_HT = 'v03_pipeline/var/test/reference_datasets/dbnsfp/1.0.ht'
TEST_EIGEN_HT = 'v03_pipeline/var/test/reference_datasets/eigen/1.0.ht'
TEST_CLINVAR_HT = 'v03_pipeline/var/test/reference_datasets/clinvar/2024-11-11.ht'
TEST_EXAC_HT = 'v03_pipeline/var/test/reference_datasets/exac/1.0.ht'
TEST_SPLICE_AI_HT = 'v03_pipeline/var/test/reference_datasets/splice_ai/1.0.ht'
TEST_TOPMED_HT = 'v03_pipeline/var/test/reference_datasets/topmed/1.0.ht'
TEST_HGMD_HT = 'v03_pipeline/var/test/reference_datasets/hgmd/1.0.ht'
TEST_GNOMAD_EXOMES_HT = 'v03_pipeline/var/test/reference_datasets/gnomad_exomes/1.0.ht'
TEST_GNOMAD_GENOMES_HT = (
    'v03_pipeline/var/test/reference_datasets/gnomad_genomes/1.0.ht'
)
TEST_GNOMAD_NONCODING_CONSTRAINT_HT = (
    'v03_pipeline/var/test/reference_datasets/gnomad_non_coding_constraint/1.0.ht'
)
TEST_SCREEN_HT = 'v03_pipeline/var/test/reference_datasets/screen/1.0.ht'
TEST_HELIX_MITO_HT = 'v03_pipeline/var/test/reference_datasets/helix_mito/1.0.ht'
TEST_HMTVAR_HT = 'v03_pipeline/var/test/reference_datasets/hmtvar/1.0.ht'
TEST_MITIMPACT_HT = 'v03_pipeline/var/test/reference_datasets/mitimpact/1.0.ht'
TEST_MITOMAP_HT = 'v03_pipeline/var/test/reference_datasets/mitomap/1.0.ht'
TEST_GNOMAD_MITO_HT = 'v03_pipeline/var/test/reference_datasets/gnomad_mito/1.0.ht'
TEST_LOCAL_CONSTRAINT_MITO_HT = (
    'v03_pipeline/var/test/reference_datasets/local_constraint_mito/1.0.ht'
)

TEST_SNV_INDEL_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'

BASE_ENUMS = {
    'sorted_motif_feature_consequences': hl.Struct(
        consequence_term=MOTIF_CONSEQUENCE_TERMS,
    ),
    'sorted_regulatory_feature_consequences': hl.Struct(
        biotype=REGULATORY_BIOTYPES,
        consequence_term=REGULATORY_CONSEQUENCE_TERMS,
    ),
    'sorted_transcript_consequences': hl.Struct(
        biotype=BIOTYPES,
        consequence_term=TRANSCRIPT_CONSEQUENCE_TERMS,
        loftee=hl.Struct(
            lof_filter=LOF_FILTERS,
        ),
        utrannotator=hl.Struct(
            fiveutr_consequence=FIVEUTR_CONSEQUENCES,
        ),
    ),
}

BASE_MITO_ENUMS = {
    'sorted_transcript_consequences': hl.Struct(
        biotype=BIOTYPES,
        consequence_term=TRANSCRIPT_CONSEQUENCE_TERMS,
        lof_filter=LOF_FILTERS,
    ),
    'mitotip': hl.Struct(
        trna_prediction=MITOTIP_PATHOGENICITIES,
    ),
}


class UpdateVATWithUpdatedRDC(MockedDatarootTestCase):
    @responses.activate
    def setUp(self) -> None:
        super().setUp()
        shutil.copytree(
            TEST_DBNSFP_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.dbnsfp,
            ),
        )
        shutil.copytree(
            TEST_EIGEN_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.eigen,
            ),
        )
        with mock_clinvar_urls():
            shutil.copytree(
                TEST_CLINVAR_HT,
                valid_reference_dataset_path(
                    ReferenceGenome.GRCh38,
                    ReferenceDataset.clinvar,
                ),
            )
        shutil.copytree(
            TEST_EXAC_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.exac,
            ),
        )
        shutil.copytree(
            TEST_SPLICE_AI_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.splice_ai,
            ),
        )
        shutil.copytree(
            TEST_TOPMED_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.topmed,
            ),
        )
        shutil.copytree(
            TEST_HGMD_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.hgmd,
            ),
        )
        shutil.copytree(
            TEST_GNOMAD_EXOMES_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.gnomad_exomes,
            ),
        )
        shutil.copytree(
            TEST_GNOMAD_GENOMES_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.gnomad_genomes,
            ),
        )
        shutil.copytree(
            TEST_GNOMAD_NONCODING_CONSTRAINT_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.gnomad_non_coding_constraint,
            ),
        )
        shutil.copytree(
            TEST_SCREEN_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.screen,
            ),
        )
        shutil.copytree(
            TEST_HELIX_MITO_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.helix_mito,
            ),
        )
        shutil.copytree(
            TEST_HMTVAR_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.hmtvar,
            ),
        )
        shutil.copytree(
            TEST_MITIMPACT_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.mitimpact,
            ),
        )
        shutil.copytree(
            TEST_MITOMAP_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.mitomap,
            ),
        )
        shutil.copytree(
            TEST_GNOMAD_MITO_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.gnomad_mito,
            ),
        )
        shutil.copytree(
            TEST_LOCAL_CONSTRAINT_MITO_HT,
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.local_constraint_mito,
            ),
        )

    def test_create_empty_annotations_table(self):
        with patch.object(
            BaseReferenceDataset,
            '_for_reference_genome_dataset_type',
            return_value=[],
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
                ht.globals.collect(),
                [
                    hl.Struct(
                        versions=hl.Struct(),
                        enums=hl.Struct(**BASE_ENUMS),
                        migrations=[],
                        updates=set(),
                    ),
                ],
            )

    @responses.activate
    @patch(
        'v03_pipeline.lib.tasks.base.base_update_variant_annotations_table.BaseUpdateVariantAnnotationsTableTask.initialize_table',
    )
    @patch(
        'v03_pipeline.lib.tasks.base.base_update_variant_annotations_table.UpdatedReferenceDatasetTask',
    )
    @patch(
        'v03_pipeline.lib.tasks.base.base_update_variant_annotations_table.UpdatedReferenceDatasetQueryTask',
    )
    def test_update_vat_snv_indel_38(
        self,
        mock_rd_query_task,
        mock_rd_task,
        mock_initialize_annotations_ht,
    ):
        mock_rd_task.return_value = MockCompleteTask()
        mock_rd_query_task.return_value = MockCompleteTask()

        mock_initialize_annotations_ht.return_value = hl.Table.parallelize(
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'C'],
                ),
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
            ),
            key=['locus', 'alleles'],
            globals=hl.Struct(
                versions=hl.Struct(),
                enums=hl.Struct(),
                updates=hl.empty_set(hl.tstruct(callset=hl.tstr, project_guid=hl.tstr)),
                migrations=hl.empty_array(hl.tstr),
            ),
        )

        with mock_clinvar_urls():
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
                ht.globals.collect(),
                [
                    hl.Struct(
                        versions=hl.Struct(
                            dbnsfp='1.0',
                            eigen='1.0',
                            clinvar='2024-11-11',
                            exac='1.0',
                            splice_ai='1.0',
                            topmed='1.0',
                            hgmd='1.0',
                            gnomad_exomes='1.0',
                            gnomad_genomes='1.0',
                            screen='1.0',
                            gnomad_non_coding_constraint='1.0',
                        ),
                        enums=hl.Struct(
                            dbnsfp=ReferenceDataset.dbnsfp.enum_globals,
                            eigen=hl.Struct(),
                            clinvar=ReferenceDataset.clinvar.enum_globals,
                            exac=hl.Struct(),
                            splice_ai=ReferenceDataset.splice_ai.enum_globals,
                            topmed=hl.Struct(),
                            hgmd=ReferenceDataset.hgmd.enum_globals,
                            gnomad_exomes=hl.Struct(),
                            gnomad_genomes=hl.Struct(),
                            screen=ReferenceDataset.screen.enum_globals,
                            gnomad_non_coding_constraint=hl.Struct(),
                            **BASE_ENUMS,
                        ),
                        migrations=[],
                        updates=set(),
                    ),
                ],
            )
            self.assertCountEqual(
                ht.collect(),
                [
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chr1',
                            position=871269,
                            reference_genome='GRCh38',
                        ),
                        alleles=['A', 'C'],
                        dbnsfp=hl.Struct(
                            REVEL_score=0.0430000014603138,
                            SIFT_score=None,
                            Polyphen2_HVAR_score=None,
                            MutationTaster_pred_id=0,
                            VEST4_score=None,
                            MutPred_score=None,
                            fathmm_MKL_coding_score=None,
                            MPC_score=None,
                            CADD_phred=2,
                            PrimateAI_score=None,
                        ),
                        eigen=hl.Struct(Eigen_phred=1.5880000591278076),
                        clinvar=hl.Struct(
                            alleleId=None,
                            conflictingPathogenicities=None,
                            goldStars=None,
                            pathogenicity_id=None,
                            assertion_ids=None,
                            submitters=None,
                            conditions=None,
                        ),
                        exac=hl.Struct(
                            AF_POPMAX=0.0004100881633348763,
                            AF=0.0004633000062312931,
                            AC_Adj=51,
                            AC_Het=51,
                            AC_Hom=0,
                            AC_Hemi=None,
                            AN_Adj=108288,
                        ),
                        splice_ai=hl.Struct(
                            delta_score=0.029999999329447746,
                            splice_consequence_id=3,
                        ),
                        topmed=hl.Struct(AC=None, AF=None, AN=None, Hom=None, Het=None),
                        hgmd=hl.Struct(accession='abcdefg', class_id=3),
                        gnomad_exomes=hl.Struct(
                            AF=0.00012876000255346298,
                            AN=240758,
                            AC=31,
                            Hom=0,
                            AF_POPMAX_OR_GLOBAL=0.0001119549197028391,
                            FAF_AF=9.315000352216884e-05,
                            Hemi=0,
                        ),
                        gnomad_genomes=hl.Struct(
                            AC=None,
                            AF=None,
                            AN=None,
                            Hom=None,
                            AF_POPMAX_OR_GLOBAL=None,
                            FAF_AF=None,
                            Hemi=None,
                        ),
                        gnomad_non_coding_constraint=hl.Struct(z_score=0.75),
                        screen=hl.Struct(region_type_ids=[1]),
                    ),
                ],
            )

    @responses.activate
    @patch(
        'v03_pipeline.lib.tasks.base.base_update_variant_annotations_table.BaseUpdateVariantAnnotationsTableTask.initialize_table',
    )
    @patch(
        'v03_pipeline.lib.tasks.base.base_update_variant_annotations_table.UpdatedReferenceDatasetTask',
    )
    @patch(
        'v03_pipeline.lib.tasks.base.base_update_variant_annotations_table.UpdatedReferenceDatasetQueryTask',
    )
    def test_update_vat_mito_38(
        self,
        mock_rd_query_task,
        mock_rd_task,
        mock_initialize_annotations_ht,
    ):
        mock_rd_task.return_value = MockCompleteTask()
        mock_rd_query_task.return_value = MockCompleteTask()

        mock_initialize_annotations_ht.return_value = hl.Table.parallelize(
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'C'],
                ),
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
            ),
            key=['locus', 'alleles'],
            globals=hl.Struct(
                versions=hl.Struct(),
                enums=hl.Struct(),
                updates=hl.empty_set(hl.tstruct(callset=hl.tstr, project_guid=hl.tstr)),
                migrations=hl.empty_array(hl.tstr),
            ),
        )

        with mock_clinvar_urls():
            task = UpdateVariantAnnotationsTableWithUpdatedReferenceDataset(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.MITO,
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
                ht.globals.collect(),
                [
                    hl.Struct(
                        versions=hl.Struct(
                            helix_mito='1.0',
                            hmtvar='1.0',
                            mitimpact='1.0',
                            mitomap='1.0',
                            gnomad_mito='1.0',
                            local_constraint_mito='1.0',
                            clinvar='2024-11-11',
                            dbnsfp='1.0',
                        ),
                        enums=hl.Struct(
                            helix_mito=hl.Struct(),
                            hmtvar=hl.Struct(),
                            mitimpact=hl.Struct(),
                            mitomap=hl.Struct(),
                            gnomad_mito=hl.Struct(),
                            local_constraint_mito=hl.Struct(),
                            clinvar=ReferenceDataset.clinvar.enum_globals,
                            dbnsfp=ReferenceDataset.dbnsfp.enum_globals,
                            **BASE_MITO_ENUMS,
                        ),
                        migrations=[],
                        updates=set(),
                    ),
                ],
            )
            self.assertCountEqual(
                ht.collect(),
                [
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chrM',
                            position=1,
                            reference_genome='GRCh38',
                        ),
                        alleles=['A', 'C'],
                        helix_mito=hl.Struct(
                            AC_het=0,
                            AF_het=0.0,
                            AN=195982,
                            max_hl=None,
                            AC_hom=0,
                            AF_hom=0,
                        ),
                        hmtvar=hl.Struct(score=0.6700000166893005),
                        mitimpact=hl.Struct(score=0.42500001192092896),
                        mitomap=hl.Struct(pathogenic=None),
                        gnomad_mito=hl.Struct(
                            AC_het=0,
                            AF_het=0.0,
                            AN=195982,
                            max_hl=None,
                            AC_hom=0,
                            AF_hom=0,
                        ),
                        local_constraint_mito=hl.Struct(score=0.5),
                        clinvar=hl.Struct(
                            alleleId=None,
                            conflictingPathogenicities=None,
                            goldStars=None,
                            pathogenicity_id=None,
                            assertion_ids=None,
                            submitters=None,
                            conditions=None,
                        ),
                        dbnsfp=hl.Struct(
                            SIFT_score=None,
                            MutationTaster_pred_id=2,
                            REVEL_score=None,
                            Polyphen2_HVAR_score=None,
                            VEST4_score=None,
                            MutPred_score=None,
                            fathmm_MKL_coding_score=None,
                            MPC_score=None,
                            CADD_phred=None,
                            PrimateAI_score=None,
                        ),
                    ),
                ],
            )
