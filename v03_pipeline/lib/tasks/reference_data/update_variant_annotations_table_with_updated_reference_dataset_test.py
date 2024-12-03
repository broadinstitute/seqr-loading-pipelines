from unittest.mock import patch

import hail as hl
import luigi.worker
import responses

from v03_pipeline.lib.annotations.enums import (
    BIOTYPES,
    CLINVAR_ASSERTIONS,
    CLINVAR_PATHOGENICITIES,
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
)
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    BaseReferenceDataset,
    ReferenceDataset,
)
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget
from v03_pipeline.lib.tasks.reference_data.update_variant_annotations_table_with_updated_reference_dataset import (
    UpdateVariantAnnotationsTableWithUpdatedReferenceDataset,
)
from v03_pipeline.lib.test.mock_clinvar_urls import mock_clinvar_urls
from v03_pipeline.lib.test.mocked_reference_datasets_testcase import (
    MockedReferenceDatasetsTestCase,
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


class UpdateVATWithUpdatedReferenceDatasets(MockedReferenceDatasetsTestCase):
    @responses.activate
    def test_create_empty_annotations_table(self):
        with patch.object(
            BaseReferenceDataset,
            'for_reference_genome_dataset_type_annotations',
            return_value=[ReferenceDataset.clinvar],
        ), mock_clinvar_urls(ReferenceGenome.GRCh38):
            task = UpdateVariantAnnotationsTableWithUpdatedReferenceDataset(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SNV_INDEL,
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
                        versions=hl.Struct(clinvar='2024-11-11'),
                        enums=hl.Struct(
                            clinvar=hl.Struct(
                                assertion=CLINVAR_ASSERTIONS,
                                pathogenicity=CLINVAR_PATHOGENICITIES,
                            ),
                            **BASE_ENUMS,
                        ),
                        migrations=[],
                        updates=set(),
                    ),
                ],
            )

    @responses.activate
    @patch(
        'v03_pipeline.lib.tasks.base.base_update_variant_annotations_table.BaseUpdateVariantAnnotationsTableTask.initialize_table',
    )
    def test_update_vat_snv_indel_38(
        self,
        mock_initialize_annotations_ht,
    ):
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
                            eigen='1.1',
                            clinvar='2024-11-11',
                            exac='1.1',
                            splice_ai='1.1',
                            topmed='1.1',
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
    def test_update_vat_mito_38(
        self,
        mock_initialize_annotations_ht,
    ):
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
                            hmtvar='1.1',
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
                            sorted_transcript_consequences=hl.Struct(
                                biotype=BIOTYPES,
                                consequence_term=TRANSCRIPT_CONSEQUENCE_TERMS,
                                lof_filter=LOF_FILTERS,
                            ),
                            mitotip=hl.Struct(
                                trna_prediction=MITOTIP_PATHOGENICITIES,
                            ),
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
                        ),
                    ),
                ],
            )

    @responses.activate
    @patch(
        'v03_pipeline.lib.tasks.base.base_update_variant_annotations_table.BaseUpdateVariantAnnotationsTableTask.initialize_table',
    )
    def test_update_vat_snv_indel_37(
        self,
        mock_initialize_annotations_ht,
    ):
        mock_initialize_annotations_ht.return_value = hl.Table.parallelize(
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig=1,
                        position=871269,
                        reference_genome='GRCh37',
                    ),
                    alleles=['A', 'C'],
                ),
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh37'),
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

        with mock_clinvar_urls(ReferenceGenome.GRCh37):
            task = UpdateVariantAnnotationsTableWithUpdatedReferenceDataset(
                reference_genome=ReferenceGenome.GRCh37,
                dataset_type=DatasetType.SNV_INDEL,
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
                            eigen='1.1',
                            clinvar='2024-11-11',
                            exac='1.1',
                            splice_ai='1.1',
                            topmed='1.1',
                            hgmd='1.0',
                            gnomad_exomes='1.0',
                            gnomad_genomes='1.0',
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
                            sorted_transcript_consequences=hl.Struct(
                                biotype=BIOTYPES,
                                consequence_term=TRANSCRIPT_CONSEQUENCE_TERMS,
                                lof_filter=LOF_FILTERS,
                            ),
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
                            contig=1,
                            position=871269,
                            reference_genome='GRCh37',
                        ),
                        alleles=['A', 'C'],
                        dbnsfp=hl.Struct(
                            REVEL_score=0.0430000014603138,
                            SIFT_score=None,
                            Polyphen2_HVAR_score=None,
                            MutationTaster_pred_id=0,
                            CADD_phred=9.699999809265137,
                            MPC_score=None,
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
                        hgmd=None,
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
                    ),
                ],
            )
