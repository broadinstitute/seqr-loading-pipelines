import shutil
from functools import partial
from unittest.mock import Mock, PropertyMock, patch

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
    SV_CONSEQUENCE_RANKS,
    SV_TYPE_DETAILS,
    SV_TYPES,
    TRANSCRIPT_CONSEQUENCE_TERMS,
)
from v03_pipeline.lib.misc.io import remap_pedigree_hash
from v03_pipeline.lib.misc.validation import validate_expected_contig_frequency
from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.paths import (
    valid_reference_dataset_path,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset
from v03_pipeline.lib.tasks.base.base_update_variant_annotations_table import (
    BaseUpdateVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget
from v03_pipeline.lib.tasks.update_variant_annotations_table_with_new_samples import (
    UpdateVariantAnnotationsTableWithNewSamplesTask,
)
from v03_pipeline.lib.test.mock_clinvar_urls import mock_clinvar_urls
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_reference_datasets_testcase import (
    MockedReferenceDatasetsTestCase,
)
from v03_pipeline.var.test.vep.mock_vep_data import MOCK_37_VEP_DATA, MOCK_38_VEP_DATA

TEST_MITO_MT = 'v03_pipeline/var/test/callsets/mito_1.mt'
TEST_SNV_INDEL_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_SV_VCF = 'v03_pipeline/var/test/callsets/sv_1.vcf'
TEST_SV_VCF_2 = 'v03_pipeline/var/test/callsets/sv_2.vcf'
TEST_GCNV_BED_FILE = 'v03_pipeline/var/test/callsets/gcnv_1.tsv'
TEST_GCNV_BED_FILE_2 = 'v03_pipeline/var/test/callsets/gcnv_2.tsv'
TEST_PEDIGREE_3_REMAP = 'v03_pipeline/var/test/pedigrees/test_pedigree_3_remap.tsv'
TEST_PEDIGREE_4_REMAP = 'v03_pipeline/var/test/pedigrees/test_pedigree_4_remap.tsv'
TEST_PEDIGREE_5 = 'v03_pipeline/var/test/pedigrees/test_pedigree_5.tsv'
TEST_PEDIGREE_8 = 'v03_pipeline/var/test/pedigrees/test_pedigree_8.tsv'
TEST_PEDIGREE_10 = 'v03_pipeline/var/test/pedigrees/test_pedigree_10.tsv'
TEST_PEDIGREE_11 = 'v03_pipeline/var/test/pedigrees/test_pedigree_11.tsv'

GENE_ID_MAPPING = {
    'OR4F5': 'ENSG00000186092',
    'PLEKHG4B': 'ENSG00000153404',
    'OR4F16': 'ENSG00000186192',
    'OR4F29': 'ENSG00000284733',
    'FBXO28': 'ENSG00000143756',
    'SAMD11': 'ENSG00000187634',
    'C1orf174': 'ENSG00000198912',
    'TAS1R1': 'ENSG00000173662',
    'FAM131C': 'ENSG00000185519',
    'RCC2': 'ENSG00000179051',
    'NBPF3': 'ENSG00000142794',
    'AGBL4': 'ENSG00000186094',
    'KIAA1614': 'ENSG00000135835',
    'MR1': 'ENSG00000153029',
    'STX6': 'ENSG00000135823',
    'XPR1': 'ENSG00000143324',
}

TEST_RUN_ID = 'manual__2024-04-03'


class UpdateVariantAnnotationsTableWithNewSamplesTaskTest(
    MockedReferenceDatasetsTestCase,
):
    @responses.activate
    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.UpdateVariantAnnotationsTableWithUpdatedReferenceDataset',
    )
    def test_missing_pedigree(
        self,
        mock_update_vat_with_rdc_task,
    ) -> None:
        with mock_clinvar_urls():
            mock_update_vat_with_rdc_task.return_value = MockCompleteTask()
            uvatwns_task = UpdateVariantAnnotationsTableWithNewSamplesTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SNV_INDEL,
                sample_type=SampleType.WGS,
                callset_path=TEST_SNV_INDEL_VCF,
                project_guids=['R0113_test_project'],
                project_pedigree_paths=['bad_pedigree'],
                skip_validation=True,
                run_id=TEST_RUN_ID,
            )
            worker = luigi.worker.Worker()
            worker.add(uvatwns_task)
            worker.run()
            self.assertFalse(uvatwns_task.complete())

    @responses.activate
    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.UpdateVariantAnnotationsTableWithUpdatedReferenceDataset',
    )
    def test_missing_interval_reference_dataset(
        self,
        mock_update_vat_with_rd_task,
    ) -> None:
        with mock_clinvar_urls():
            mock_update_vat_with_rd_task.return_value = MockCompleteTask()
            shutil.rmtree(
                valid_reference_dataset_path(
                    ReferenceGenome.GRCh38,
                    ReferenceDataset.screen,
                ),
            )
            uvatwns_task = UpdateVariantAnnotationsTableWithNewSamplesTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SNV_INDEL,
                sample_type=SampleType.WGS,
                callset_path=TEST_SNV_INDEL_VCF,
                project_guids=['R0113_test_project'],
                project_pedigree_paths=[TEST_PEDIGREE_3_REMAP],
                skip_validation=True,
                run_id=TEST_RUN_ID,
            )
            worker = luigi.worker.Worker()
            worker.add(uvatwns_task)
            worker.run()
            self.assertFalse(uvatwns_task.complete())

    @responses.activate
    @patch(
        'v03_pipeline.lib.tasks.update_new_variants_with_caids.register_alleles_in_chunks',
    )
    @patch('v03_pipeline.lib.tasks.update_new_variants_with_caids.Env')
    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.UpdateVariantAnnotationsTableWithUpdatedReferenceDataset',
    )
    @patch(
        'v03_pipeline.lib.tasks.validate_callset.validate_expected_contig_frequency',
        partial(validate_expected_contig_frequency, min_rows_per_contig=25),
    )
    @patch.object(ReferenceGenome, 'standard_contigs', new_callable=PropertyMock)
    @patch('v03_pipeline.lib.vep.hl.vep')
    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.load_gencode_ensembl_to_refseq_id',
    )
    def test_multiple_update_vat(
        self,
        mock_load_gencode_ensembl_to_refseq_id: Mock,
        mock_vep: Mock,
        mock_standard_contigs: Mock,
        mock_update_vat_with_rd_task: Mock,
        mock_env_caids: Mock,
        mock_register_alleles: Mock,
    ) -> None:
        mock_update_vat_with_rd_task.return_value = (
            BaseUpdateVariantAnnotationsTableTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SNV_INDEL,
            )
        )
        mock_vep.side_effect = lambda ht, **_: ht.annotate(vep=MOCK_38_VEP_DATA)
        mock_load_gencode_ensembl_to_refseq_id.return_value = hl.dict(
            {'ENST00000327044': 'NM_015658.4'},
        )
        # make register_alleles return CAIDs for 4 of 30 variants
        mock_env_caids.CLINGEN_ALLELE_REGISTRY_LOGIN = 'login'
        mock_env_caids.CLINGEN_ALLELE_REGISTRY_PASSWORD = 'password1'  # noqa: S105
        mock_register_alleles.side_effect = [
            iter(
                [
                    hl.Table.parallelize(
                        [
                            hl.Struct(
                                locus=hl.Locus(
                                    contig='chr1',
                                    position=871269,
                                    reference_genome='GRCh38',
                                ),
                                alleles=['A', 'C'],
                                CAID='CA1',
                            ),
                            hl.Struct(
                                locus=hl.Locus(
                                    contig='chr1',
                                    position=874734,
                                    reference_genome='GRCh38',
                                ),
                                alleles=['C', 'T'],
                                CAID='CA2',
                            ),
                            hl.Struct(
                                locus=hl.Locus(
                                    contig='chr1',
                                    position=876499,
                                    reference_genome='GRCh38',
                                ),
                                alleles=['A', 'G'],
                                CAID='CA3',
                            ),
                            hl.Struct(
                                locus=hl.Locus(
                                    contig='chr1',
                                    position=878314,
                                    reference_genome='GRCh38',
                                ),
                                alleles=['G', 'C'],
                                CAID='CA4',
                            ),
                        ],
                        hl.tstruct(
                            locus=hl.tlocus('GRCh38'),
                            alleles=hl.tarray(hl.tstr),
                            CAID=hl.tstr,
                        ),
                        key=('locus', 'alleles'),
                    ),
                ],
            ),
            iter(
                [],
            ),  # for the second call, there are no new variants, return empty iterator
        ]

        mock_standard_contigs.return_value = {'chr1'}
        # This creates a mock validation table with 1 coding and 1 non-coding variant
        # explicitly chosen from the VCF.
        # NB: This is the one and only place validation is enabled in the task tests!
        coding_and_noncoding_variants_ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    'coding': False,
                    'noncoding': True,
                },
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=876499,
                        reference_genome='GRCh38',
                    ),
                    'coding': True,
                    'noncoding': False,
                },
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                coding=hl.tbool,
                noncoding=hl.tbool,
            ),
            key='locus',
            globals=hl.Struct(
                versions=hl.Struct(
                    gnomad_genomes='1.0',
                ),
                enums=hl.Struct(
                    gnomad_genomes=hl.Struct(),
                ),
            ),
        )

        with mock_clinvar_urls():
            coding_and_noncoding_variants_ht.write(
                valid_reference_dataset_path(
                    ReferenceGenome.GRCh38,
                    ReferenceDataset.gnomad_coding_and_noncoding,
                ),
                overwrite=True,
            )
            worker = luigi.worker.Worker()

            uvatwns_task_3 = UpdateVariantAnnotationsTableWithNewSamplesTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SNV_INDEL,
                sample_type=SampleType.WGS,
                callset_path=TEST_SNV_INDEL_VCF,
                project_guids=['R0113_test_project'],
                project_pedigree_paths=[TEST_PEDIGREE_3_REMAP],
                skip_validation=False,
                run_id=TEST_RUN_ID,
            )
            worker.add(uvatwns_task_3)
            worker.run()
            self.assertTrue(uvatwns_task_3.complete())
            ht = hl.read_table(uvatwns_task_3.output().path)
            self.assertEqual(ht.count(), 30)
            self.assertEqual(
                [
                    x
                    for x in ht.select(
                        'gt_stats',
                        'CAID',
                    ).collect()
                    if x.locus.position <= 871269  # noqa: PLR2004
                ],
                [
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chr1',
                            position=871269,
                            reference_genome='GRCh38',
                        ),
                        alleles=['A', 'C'],
                        gt_stats=hl.Struct(AC=0, AN=6, AF=0.0, hom=0),
                        CAID='CA1',
                    ),
                ],
            )
            self.assertEqual(
                ht.globals.updates.collect(),
                [
                    {
                        hl.Struct(
                            callset=TEST_SNV_INDEL_VCF,
                            project_guid='R0113_test_project',
                            remap_pedigree_hash=hl.eval(
                                remap_pedigree_hash(TEST_PEDIGREE_3_REMAP),
                            ),
                        ),
                    },
                ],
            )
            self.assertEqual(
                hl.eval(ht.globals.max_key_),
                29,
            )

            # Ensure that new variants are added correctly to the table.
            uvatwns_task_4 = UpdateVariantAnnotationsTableWithNewSamplesTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SNV_INDEL,
                sample_type=SampleType.WGS,
                callset_path=TEST_SNV_INDEL_VCF,
                project_guids=['R0114_project4'],
                project_pedigree_paths=[TEST_PEDIGREE_4_REMAP],
                skip_validation=False,
                run_id=TEST_RUN_ID + '-another-run',
            )
            worker.add(uvatwns_task_4)
            worker.run()
            self.assertTrue(uvatwns_task_4.complete())
            ht = hl.read_table(uvatwns_task_4.output().path)
            self.assertCountEqual(
                [
                    x
                    for x in ht.select(
                        'clinvar',
                        'hgmd',
                        'variant_id',
                        'xpos',
                        'gt_stats',
                        'screen',
                        'CAID',
                    ).collect()
                    if x.locus.position <= 878809  # noqa: PLR2004
                ],
                [
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chr1',
                            position=871269,
                            reference_genome='GRCh38',
                        ),
                        alleles=['A', 'C'],
                        clinvar=hl.Struct(
                            alleleId=None,
                            conflictingPathogenicities=None,
                            goldStars=None,
                            pathogenicity_id=None,
                            assertion_ids=None,
                            submitters=None,
                            conditions=None,
                        ),
                        hgmd=hl.Struct(
                            accession='abcdefg',
                            class_id=3,
                        ),
                        variant_id='1-871269-A-C',
                        xpos=1000871269,
                        gt_stats=hl.Struct(AC=1, AN=32, AF=0.03125, hom=0),
                        screen=hl.Struct(region_type_ids=[1]),
                        CAID='CA1',
                    ),
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chr1',
                            position=874734,
                            reference_genome='GRCh38',
                        ),
                        alleles=['C', 'T'],
                        clinvar=None,
                        hgmd=None,
                        variant_id='1-874734-C-T',
                        xpos=1000874734,
                        gt_stats=hl.Struct(AC=1, AN=32, AF=0.03125, hom=0),
                        screen=hl.Struct(region_type_ids=[]),
                        CAID='CA2',
                    ),
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chr1',
                            position=876499,
                            reference_genome='GRCh38',
                        ),
                        alleles=['A', 'G'],
                        clinvar=None,
                        hgmd=None,
                        variant_id='1-876499-A-G',
                        xpos=1000876499,
                        gt_stats=hl.Struct(AC=31, AN=32, AF=0.96875, hom=15),
                        screen=hl.Struct(region_type_ids=[]),
                        CAID='CA3',
                    ),
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chr1',
                            position=878314,
                            reference_genome='GRCh38',
                        ),
                        alleles=['G', 'C'],
                        clinvar=None,
                        hgmd=None,
                        variant_id='1-878314-G-C',
                        xpos=1000878314,
                        gt_stats=hl.Struct(AC=3, AN=32, AF=0.09375, hom=0),
                        screen=hl.Struct(region_type_ids=[]),
                        CAID='CA4',
                    ),
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chr1',
                            position=878809,
                            reference_genome='GRCh38',
                        ),
                        alleles=['C', 'T'],
                        clinvar=None,
                        hgmd=None,
                        variant_id='1-878809-C-T',
                        xpos=1000878809,
                        gt_stats=hl.Struct(AC=1, AN=32, AF=0.03125, hom=0),
                        screen=hl.Struct(region_type_ids=[]),
                        CAID=None,
                    ),
                ],
            )
            self.assertCountEqual(
                ht.filter(
                    ht.locus.position <= 878809,  # noqa: PLR2004
                ).sorted_transcript_consequences.consequence_term_ids.collect(),
                [
                    [[9], [23, 26], [23, 13, 26]],
                    [[9], [23, 26], [23, 13, 26]],
                    [[9], [23, 26], [23, 13, 26]],
                    [[9], [23, 26], [23, 13, 26]],
                    [[9], [23, 26], [23, 13, 26]],
                ],
            )
            self.assertCountEqual(
                ht.globals.collect(),
                [
                    hl.Struct(
                        updates={
                            hl.Struct(
                                callset='v03_pipeline/var/test/callsets/1kg_30variants.vcf',
                                project_guid='R0113_test_project',
                                remap_pedigree_hash=hl.eval(
                                    remap_pedigree_hash(
                                        TEST_PEDIGREE_3_REMAP,
                                    ),
                                ),
                            ),
                            hl.Struct(
                                callset='v03_pipeline/var/test/callsets/1kg_30variants.vcf',
                                project_guid='R0114_project4',
                                remap_pedigree_hash=hl.eval(
                                    remap_pedigree_hash(
                                        TEST_PEDIGREE_4_REMAP,
                                    ),
                                ),
                            ),
                        },
                        versions=hl.Struct(
                            clinvar='2024-11-11',
                            dbnsfp='1.0',
                            eigen='1.1',
                            exac='1.1',
                            gnomad_exomes='1.0',
                            gnomad_genomes='1.0',
                            splice_ai='1.1',
                            topmed='1.1',
                            gnomad_non_coding_constraint='1.0',
                            screen='1.0',
                            hgmd='1.0',
                        ),
                        migrations=[],
                        max_key_=29,
                        enums=hl.Struct(
                            clinvar=ReferenceDataset.clinvar.enum_globals,
                            dbnsfp=ReferenceDataset.dbnsfp.enum_globals,
                            eigen=hl.Struct(),
                            exac=hl.Struct(),
                            gnomad_exomes=hl.Struct(),
                            gnomad_genomes=hl.Struct(),
                            splice_ai=ReferenceDataset.splice_ai.enum_globals,
                            topmed=hl.Struct(),
                            hgmd=ReferenceDataset.hgmd.enum_globals,
                            gnomad_non_coding_constraint=hl.Struct(),
                            screen=ReferenceDataset.screen.enum_globals,
                            sorted_motif_feature_consequences=hl.Struct(
                                consequence_term=MOTIF_CONSEQUENCE_TERMS,
                            ),
                            sorted_regulatory_feature_consequences=hl.Struct(
                                biotype=REGULATORY_BIOTYPES,
                                consequence_term=REGULATORY_CONSEQUENCE_TERMS,
                            ),
                            sorted_transcript_consequences=hl.Struct(
                                biotype=BIOTYPES,
                                consequence_term=TRANSCRIPT_CONSEQUENCE_TERMS,
                                loftee=hl.Struct(
                                    lof_filter=LOF_FILTERS,
                                ),
                                utrannotator=hl.Struct(
                                    fiveutr_consequence=FIVEUTR_CONSEQUENCES,
                                ),
                            ),
                        ),
                    ),
                ],
            )

    @responses.activate
    @patch(
        'v03_pipeline.lib.tasks.update_new_variants_with_caids.register_alleles_in_chunks',
    )
    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.UpdateVariantAnnotationsTableWithUpdatedReferenceDataset',
    )
    @patch('v03_pipeline.lib.vep.hl.vep')
    def test_update_vat_grch37(
        self,
        mock_vep: Mock,
        mock_update_vat_with_rd_task: Mock,
        mock_register_alleles: Mock,
    ) -> None:
        mock_update_vat_with_rd_task.return_value = (
            BaseUpdateVariantAnnotationsTableTask(
                reference_genome=ReferenceGenome.GRCh37,
                dataset_type=DatasetType.SNV_INDEL,
            )
        )
        mock_vep.side_effect = lambda ht, **_: ht.annotate(vep=MOCK_37_VEP_DATA)
        mock_register_alleles.side_effect = None

        with mock_clinvar_urls(ReferenceGenome.GRCh37):
            worker = luigi.worker.Worker()
            uvatwns_task = UpdateVariantAnnotationsTableWithNewSamplesTask(
                reference_genome=ReferenceGenome.GRCh37,
                dataset_type=DatasetType.SNV_INDEL,
                sample_type=SampleType.WGS,
                callset_path=TEST_SNV_INDEL_VCF,
                project_guids=['R0113_test_project'],
                project_pedigree_paths=[TEST_PEDIGREE_3_REMAP],
                skip_validation=True,
                run_id=TEST_RUN_ID,
            )
            worker.add(uvatwns_task)
            worker.run()
            self.assertTrue(uvatwns_task.complete())
            ht = hl.read_table(uvatwns_task.output().path)
            self.assertEqual(ht.count(), 30)
            self.assertFalse(hasattr(ht, 'rg37_locus'))
            self.assertEqual(
                ht.collect()[0],
                hl.Struct(
                    locus=hl.Locus(
                        contig=1,
                        position=871269,
                        reference_genome='GRCh37',
                    ),
                    alleles=['A', 'C'],
                    rsid=None,
                    variant_id='1-871269-A-C',
                    xpos=1000871269,
                    sorted_transcript_consequences=[
                        hl.Struct(
                            amino_acids='S/L',
                            canonical=1,
                            codons='tCg/tTg',
                            gene_id='ENSG00000188976',
                            hgvsc='ENST00000327044.6:c.1667C>T',
                            hgvsp='ENSP00000317992.6:p.Ser556Leu',
                            transcript_id='ENST00000327044',
                            biotype_id=39,
                            consequence_term_ids=[9],
                            is_lof_nagnag=None,
                            lof_filter_ids=[0, 1],
                        ),
                        hl.Struct(
                            amino_acids=None,
                            canonical=None,
                            codons=None,
                            gene_id='ENSG00000188976',
                            hgvsc='ENST00000477976.1:n.3114C>T',
                            hgvsp=None,
                            transcript_id='ENST00000477976',
                            biotype_id=38,
                            consequence_term_ids=[23, 26],
                            is_lof_nagnag=None,
                            lof_filter_ids=None,
                        ),
                        hl.Struct(
                            amino_acids=None,
                            canonical=None,
                            codons=None,
                            gene_id='ENSG00000188976',
                            hgvsc='ENST00000483767.1:n.523C>T',
                            hgvsp=None,
                            transcript_id='ENST00000483767',
                            biotype_id=38,
                            consequence_term_ids=[23, 26],
                            is_lof_nagnag=None,
                            lof_filter_ids=None,
                        ),
                    ],
                    rg38_locus=hl.Locus(
                        contig='chr1',
                        position=935889,
                        reference_genome='GRCh38',
                    ),
                    clinvar=hl.Struct(
                        alleleId=None,
                        conflictingPathogenicities=None,
                        goldStars=None,
                        pathogenicity_id=None,
                        assertion_ids=None,
                        submitters=None,
                        conditions=None,
                    ),
                    eigen=hl.Struct(Eigen_phred=1.5880000591278076),
                    exac=hl.Struct(
                        AF_POPMAX=0.0004100881633348763,
                        AF=0.0004633000062312931,
                        AC_Adj=51,
                        AC_Het=51,
                        AC_Hom=0,
                        AC_Hemi=None,
                        AN_Adj=108288,
                    ),
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
                        AF=None,
                        AN=None,
                        AC=None,
                        Hom=None,
                        AF_POPMAX_OR_GLOBAL=None,
                        FAF_AF=None,
                        Hemi=None,
                    ),
                    splice_ai=hl.Struct(
                        delta_score=0.029999999329447746,
                        splice_consequence_id=3,
                    ),
                    topmed=hl.Struct(AC=None, AF=None, AN=None, Hom=None, Het=None),
                    dbnsfp=hl.Struct(
                        REVEL_score=0.0430000014603138,
                        SIFT_score=None,
                        Polyphen2_HVAR_score=None,
                        MutationTaster_pred_id=0,
                        CADD_phred=9.699999809265137,
                        MPC_score=None,
                        PrimateAI_score=None,
                    ),
                    hgmd=None,
                    gt_stats=hl.Struct(AC=0, AN=6, AF=0.0, hom=0),
                    CAID=None,
                    key_=0,
                ),
            )

    @responses.activate
    @patch(
        'v03_pipeline.lib.tasks.update_new_variants_with_caids.register_alleles_in_chunks',
    )
    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.UpdateVariantAnnotationsTableWithUpdatedReferenceDataset',
    )
    @patch('v03_pipeline.lib.reference_datasets.reference_dataset.FeatureFlag')
    @patch('v03_pipeline.lib.vep.hl.vep')
    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.load_gencode_ensembl_to_refseq_id',
    )
    def test_update_vat_without_accessing_private_datasets(
        self,
        mock_load_gencode_ensembl_to_refseq_id: Mock,
        mock_vep: Mock,
        mock_rd_ff: Mock,
        mock_update_vat_with_rd_task: Mock,
        mock_register_alleles: Mock,
    ) -> None:
        mock_load_gencode_ensembl_to_refseq_id.return_value = hl.dict(
            {'ENST00000327044': 'NM_015658.4'},
        )
        mock_update_vat_with_rd_task.return_value = (
            BaseUpdateVariantAnnotationsTableTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SNV_INDEL,
            )
        )
        shutil.rmtree(
            valid_reference_dataset_path(
                ReferenceGenome.GRCh38,
                ReferenceDataset.hgmd,
            ),
        )
        mock_rd_ff.ACCESS_PRIVATE_REFERENCE_DATASETS = False
        mock_vep.side_effect = lambda ht, **_: ht.annotate(vep=MOCK_38_VEP_DATA)
        mock_register_alleles.side_effect = None

        with mock_clinvar_urls():
            worker = luigi.worker.Worker()
            uvatwns_task = UpdateVariantAnnotationsTableWithNewSamplesTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SNV_INDEL,
                sample_type=SampleType.WGS,
                callset_path=TEST_SNV_INDEL_VCF,
                project_guids=['R0113_test_project'],
                project_pedigree_paths=[TEST_PEDIGREE_3_REMAP],
                skip_validation=True,
                run_id=TEST_RUN_ID,
            )
            worker.add(uvatwns_task)
            worker.run()
            self.assertTrue(uvatwns_task.complete())
            ht = hl.read_table(uvatwns_task.output().path)
            self.assertEqual(ht.count(), 30)
            self.assertCountEqual(
                ht.globals.versions.collect(),
                [
                    hl.Struct(
                        clinvar='2024-11-11',
                        dbnsfp='1.0',
                        eigen='1.1',
                        exac='1.1',
                        gnomad_exomes='1.0',
                        gnomad_genomes='1.0',
                        splice_ai='1.1',
                        topmed='1.1',
                        gnomad_non_coding_constraint='1.0',
                        screen='1.0',
                    ),
                ],
            )

    @responses.activate
    @patch(
        'v03_pipeline.lib.tasks.update_new_variants_with_caids.register_alleles_in_chunks',
    )
    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.UpdateVariantAnnotationsTableWithUpdatedReferenceDataset',
    )
    def test_mito_update_vat(
        self,
        mock_update_vat_with_rd_task: Mock,
        mock_register_alleles: Mock,
    ) -> None:
        mock_update_vat_with_rd_task.return_value = (
            BaseUpdateVariantAnnotationsTableTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.MITO,
            )
        )
        mock_register_alleles.side_effect = None

        with mock_clinvar_urls():
            worker = luigi.worker.Worker()
            update_variant_annotations_task = (
                UpdateVariantAnnotationsTableWithNewSamplesTask(
                    reference_genome=ReferenceGenome.GRCh38,
                    dataset_type=DatasetType.MITO,
                    sample_type=SampleType.WGS,
                    callset_path=TEST_MITO_MT,
                    project_guids=['R0115_test_project2'],
                    project_pedigree_paths=[TEST_PEDIGREE_5],
                    skip_validation=True,
                    run_id=TEST_RUN_ID,
                )
            )
            worker.add(update_variant_annotations_task)
            worker.run()
            self.assertTrue(update_variant_annotations_task.complete())
            ht = hl.read_table(update_variant_annotations_task.output().path)
            self.assertEqual(ht.count(), 5)
            self.assertCountEqual(
                ht.globals.collect(),
                [
                    hl.Struct(
                        versions=hl.Struct(
                            clinvar='2024-11-11',
                            dbnsfp='1.0',
                            gnomad_mito='1.1',
                            helix_mito='1.0',
                            hmtvar='1.1',
                            mitomap='1.0',
                            mitimpact='1.0',
                            local_constraint_mito='1.0',
                        ),
                        enums=hl.Struct(
                            local_constraint_mito=hl.Struct(),
                            clinvar=ReferenceDataset.clinvar.enum_globals,
                            dbnsfp=ReferenceDataset.dbnsfp.enum_globals,
                            gnomad_mito=hl.Struct(),
                            helix_mito=hl.Struct(),
                            hmtvar=hl.Struct(),
                            mitomap=hl.Struct(),
                            mitimpact=hl.Struct(),
                            sorted_transcript_consequences=hl.Struct(
                                biotype=BIOTYPES,
                                consequence_term=TRANSCRIPT_CONSEQUENCE_TERMS,
                                lof_filter=LOF_FILTERS,
                            ),
                            mitotip=hl.Struct(trna_prediction=MITOTIP_PATHOGENICITIES),
                        ),
                        migrations=[],
                        max_key_=4,
                        updates={
                            hl.Struct(
                                callset='v03_pipeline/var/test/callsets/mito_1.mt',
                                project_guid='R0115_test_project2',
                                remap_pedigree_hash=hl.eval(
                                    remap_pedigree_hash(
                                        TEST_PEDIGREE_5,
                                    ),
                                ),
                            ),
                        },
                    ),
                ],
            )
            self.assertCountEqual(
                ht.collect()[0],
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=3,
                        reference_genome='GRCh38',
                    ),
                    alleles=['T', 'C'],
                    common_low_heteroplasmy=False,
                    haplogroup=hl.Struct(is_defining=False),
                    mitotip=hl.Struct(trna_prediction_id=None),
                    rg37_locus=hl.Locus(
                        contig='MT',
                        position=3,
                        reference_genome='GRCh37',
                    ),
                    rsid=None,
                    sorted_transcript_consequences=None,
                    variant_id='M-3-T-C',
                    xpos=25000000003,
                    clinvar=None,
                    dbnsfp=None,
                    gnomad_mito=None,
                    helix_mito=None,
                    hmtvar=None,
                    mitomap=None,
                    mitimpact=None,
                    gt_stats=hl.Struct(
                        AC_het=1,
                        AF_het=0.25,
                        AC_hom=0,
                        AF_hom=0.0,
                        AN=4,
                    ),
                    local_constraint_mito=None,
                    key_=0,
                ),
            )

    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.load_gencode_gene_symbol_to_gene_id',
    )
    def test_sv_multiple_vcf_update_vat(
        self,
        mock_load_gencode: Mock,
    ) -> None:
        mock_load_gencode.return_value = GENE_ID_MAPPING
        worker = luigi.worker.Worker()
        update_variant_annotations_task = (
            UpdateVariantAnnotationsTableWithNewSamplesTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SV,
                sample_type=SampleType.WGS,
                callset_path=TEST_SV_VCF,
                project_guids=['R0115_test_project2'],
                project_pedigree_paths=[TEST_PEDIGREE_5],
                skip_validation=True,
                run_id=TEST_RUN_ID,
            )
        )
        worker.add(update_variant_annotations_task)
        worker.run()
        self.assertTrue(update_variant_annotations_task.complete())
        self.assertFalse(
            GCSorLocalFolderTarget(
                f'{self.mock_env.REFERENCE_DATASETS_DIR}/v03/GRCh38/SV/lookup.ht',
            ).exists(),
        )
        ht = hl.read_table(update_variant_annotations_task.output().path)
        self.assertEqual(ht.count(), 13)
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    versions=hl.Struct(gnomad_svs='1.1'),
                    enums=hl.Struct(
                        gnomad_svs=hl.Struct(),
                        sv_type=SV_TYPES,
                        sv_type_detail=SV_TYPE_DETAILS,
                        sorted_gene_consequences=hl.Struct(
                            major_consequence=SV_CONSEQUENCE_RANKS,
                        ),
                    ),
                    migrations=[],
                    max_key_=12,
                    updates={
                        hl.Struct(
                            callset=TEST_SV_VCF,
                            project_guid='R0115_test_project2',
                            remap_pedigree_hash=hl.eval(
                                remap_pedigree_hash(
                                    TEST_PEDIGREE_5,
                                ),
                            ),
                        ),
                    },
                ),
            ],
        )
        self.assertCountEqual(
            ht.collect()[:8],
            [
                hl.Struct(
                    variant_id='BND_chr1_6',
                    algorithms='manta',
                    bothsides_support=False,
                    cpx_intervals=None,
                    end_locus=hl.Locus(
                        contig='chr5',
                        position=20404,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=0.125,
                        AC=1,
                        AN=8,
                        Hom=0,
                        Het=1,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=10367,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=5,
                        position=20404,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000186092', major_consequence_id=12),
                        hl.Struct(gene_id='ENSG00000153404', major_consequence_id=12),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=180928,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_len=-1,
                    sv_type_id=2,
                    sv_type_detail_id=None,
                    xpos=1000180928,
                    key_=0,
                ),
                hl.Struct(
                    variant_id='BND_chr1_9',
                    algorithms='manta',
                    bothsides_support=False,
                    cpx_intervals=None,
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=789481,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=0.875,
                        AC=7,
                        AN=8,
                        Hom=3,
                        Het=1,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=724861,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=724861,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000143756', major_consequence_id=12),
                        hl.Struct(gene_id='ENSG00000186192', major_consequence_id=12),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=789481,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_len=223225007,
                    sv_type_id=2,
                    sv_type_detail_id=None,
                    xpos=1000789481,
                    key_=1,
                ),
                hl.Struct(
                    variant_id='CPX_chr1_22',
                    algorithms='manta',
                    bothsides_support=True,
                    cpx_intervals=[
                        hl.Struct(
                            type_id=8,
                            start=hl.Locus('chr1', 6558902, 'GRCh38'),
                            end=hl.Locus('chr1', 6559723, 'GRCh38'),
                        ),
                        hl.Struct(
                            type_id=6,
                            start=hl.Locus('chr1', 6559655, 'GRCh38'),
                            end=hl.Locus('chr1', 6559723, 'GRCh38'),
                        ),
                    ],
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=6559723,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=0.25,
                        AC=2,
                        AN=8,
                        Hom=0,
                        Het=2,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=6618962,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=6619783,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000173662', major_consequence_id=11),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=6558902,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_len=821,
                    sv_type_id=3,
                    sv_type_detail_id=2,
                    xpos=1006558902,
                    key_=2,
                ),
                hl.Struct(
                    variant_id='CPX_chr1_251',
                    algorithms='manta',
                    bothsides_support=False,
                    cpx_intervals=[
                        hl.Struct(
                            type_id=5,
                            start=hl.Locus('chr1', 180540234, 'GRCh38'),
                            end=hl.Locus('chr1', 181074767, 'GRCh38'),
                        ),
                        hl.Struct(
                            type_id=8,
                            start=hl.Locus('chr1', 181074767, 'GRCh38'),
                            end=hl.Locus('chr1', 181074938, 'GRCh38'),
                        ),
                    ],
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=181074952,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=0.375,
                        AC=3,
                        AN=8,
                        Hom=1,
                        Het=1,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=180509370,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=181044088,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000135835', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG00000153029', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG00000135823', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG00000143324', major_consequence_id=0),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=180540234,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_len=534718,
                    sv_type_id=3,
                    sv_type_detail_id=9,
                    xpos=1180540234,
                    key_=3,
                ),
                hl.Struct(
                    variant_id='CPX_chr1_41',
                    algorithms='manta',
                    bothsides_support=False,
                    cpx_intervals=[
                        hl.Struct(
                            type_id=6,
                            start=hl.Locus('chr1', 16088760, 'GRCh38'),
                            end=hl.Locus('chr1', 16088835, 'GRCh38'),
                        ),
                        hl.Struct(
                            type_id=8,
                            start=hl.Locus('chr1', 16088760, 'GRCh38'),
                            end=hl.Locus('chr1', 16089601, 'GRCh38'),
                        ),
                    ],
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=16089601,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=0.25,
                        AC=2,
                        AN=8,
                        Hom=0,
                        Het=2,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=16415255,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=16416096,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000185519', major_consequence_id=12),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=16088760,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_len=841,
                    sv_type_id=3,
                    sv_type_detail_id=12,
                    xpos=1016088760,
                    key_=4,
                ),
                hl.Struct(
                    variant_id='CPX_chr1_54',
                    algorithms='manta',
                    bothsides_support=False,
                    cpx_intervals=[
                        hl.Struct(
                            type_id=6,
                            start=hl.Locus('chr1', 21427498, 'GRCh38'),
                            end=hl.Locus('chr1', 21427959, 'GRCh38'),
                        ),
                        hl.Struct(
                            type_id=8,
                            start=hl.Locus('chr1', 21427498, 'GRCh38'),
                            end=hl.Locus('chr1', 21480073, 'GRCh38'),
                        ),
                        hl.Struct(
                            type_id=5,
                            start=hl.Locus('chr1', 21480073, 'GRCh38'),
                            end=hl.Locus('chr1', 21480419, 'GRCh38'),
                        ),
                    ],
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=21480419,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=0.5,
                        AC=4,
                        AN=8,
                        Hom=0,
                        Het=4,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=21753991,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=21806912,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000142794', major_consequence_id=0),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=21427498,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_len=52921,
                    sv_type_id=3,
                    sv_type_detail_id=13,
                    xpos=1021427498,
                    key_=5,
                ),
                hl.Struct(
                    variant_id='CPX_chrX_251',
                    algorithms='manta',
                    bothsides_support=False,
                    cpx_intervals=[
                        hl.Struct(
                            type_id=5,
                            start=hl.Locus(
                                contig='chr1',
                                position=180540234,
                                reference_genome='GRCh38',
                            ),
                            end=hl.Locus(
                                contig='chr1',
                                position=181074767,
                                reference_genome='GRCh38',
                            ),
                        ),
                        hl.Struct(
                            type_id=8,
                            start=hl.Locus(
                                contig='chr1',
                                position=181074767,
                                reference_genome='GRCh38',
                            ),
                            end=hl.Locus(
                                contig='chr1',
                                position=181074938,
                                reference_genome='GRCh38',
                            ),
                        ),
                    ],
                    end_locus=hl.Locus(
                        contig='chrX',
                        position=2781000,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=0.375,
                        AC=3,
                        AN=8,
                        Hom=1,
                        Het=1,
                    ),
                    gnomad_svs=None,
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000135835', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG00000153029', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG00000135823', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG00000143324', major_consequence_id=0),
                    ],
                    start_locus=hl.Locus(
                        contig='chrX',
                        position=3,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_type_id=3,
                    sv_type_detail_id=9,
                    sv_len=534718,
                    xpos=23000000003,
                    rg37_locus=None,
                    rg37_locus_end=hl.Locus(
                        contig='X',
                        position=2699041,
                        reference_genome='GRCh37',
                    ),
                    key_=6,
                ),
                hl.Struct(
                    variant_id='CPX_chrX_252',
                    algorithms='manta',
                    bothsides_support=False,
                    cpx_intervals=[
                        hl.Struct(
                            type_id=5,
                            start=hl.Locus(
                                contig='chr1',
                                position=180540234,
                                reference_genome='GRCh38',
                            ),
                            end=hl.Locus(
                                contig='chr1',
                                position=181074767,
                                reference_genome='GRCh38',
                            ),
                        ),
                        hl.Struct(
                            type_id=8,
                            start=hl.Locus(
                                contig='chr1',
                                position=181074767,
                                reference_genome='GRCh38',
                            ),
                            end=hl.Locus(
                                contig='chr1',
                                position=181074938,
                                reference_genome='GRCh38',
                            ),
                        ),
                    ],
                    end_locus=hl.Locus(
                        contig='chrX',
                        position=2781900,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=hl.eval(hl.float32(0.4285714328289032)),
                        AC=3,
                        AN=7,
                        Hom=1,
                        Het=1,
                    ),
                    gnomad_svs=None,
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000135835', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG00000153029', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG00000135823', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG00000143324', major_consequence_id=0),
                    ],
                    start_locus=hl.Locus(
                        contig='chrX',
                        position=2781700,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_type_id=3,
                    sv_type_detail_id=9,
                    sv_len=534718,
                    xpos=23002781700,
                    rg37_locus=hl.Locus(
                        contig='X',
                        position=2699741,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig='X',
                        position=2699941,
                        reference_genome='GRCh37',
                    ),
                    key_=7,
                ),
            ],
        )
        update_variant_annotations_task = (
            UpdateVariantAnnotationsTableWithNewSamplesTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SV,
                sample_type=SampleType.WGS,
                callset_path=TEST_SV_VCF_2,
                project_guids=['R0115_test_project2'],
                project_pedigree_paths=[TEST_PEDIGREE_5],
                skip_validation=True,
                run_id='second_run_id',
            )
        )
        worker.add(update_variant_annotations_task)
        worker.run()
        self.assertTrue(update_variant_annotations_task.complete())
        ht = hl.read_table(update_variant_annotations_task.output().path)
        self.assertEqual(ht.count(), 14)
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    versions=hl.Struct(gnomad_svs='1.1'),
                    enums=hl.Struct(
                        gnomad_svs=hl.Struct(),
                        sv_type=SV_TYPES,
                        sv_type_detail=SV_TYPE_DETAILS,
                        sorted_gene_consequences=hl.Struct(
                            major_consequence=SV_CONSEQUENCE_RANKS,
                        ),
                    ),
                    migrations=[],
                    max_key_=13,
                    updates={
                        hl.Struct(
                            callset=TEST_SV_VCF,
                            project_guid='R0115_test_project2',
                            remap_pedigree_hash=hl.eval(
                                remap_pedigree_hash(
                                    TEST_PEDIGREE_5,
                                ),
                            ),
                        ),
                        hl.Struct(
                            callset=TEST_SV_VCF_2,
                            project_guid='R0115_test_project2',
                            remap_pedigree_hash=hl.eval(
                                remap_pedigree_hash(
                                    TEST_PEDIGREE_5,
                                ),
                            ),
                        ),
                    },
                ),
            ],
        )
        self.assertCountEqual(
            ht.take(1),
            [
                hl.Struct(
                    variant_id='BND_chr1_6',
                    algorithms='manta',
                    bothsides_support=False,
                    cpx_intervals=None,
                    end_locus=hl.Locus(
                        contig='chr5',
                        position=20404,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=0.125,
                        AC=2,
                        AN=16,
                        Hom=0,
                        Het=2,
                    ),
                    gnomad_svs=None,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=10367,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=5,
                        position=20404,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000186092', major_consequence_id=12),
                        hl.Struct(gene_id='ENSG00000153404', major_consequence_id=12),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=180928,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=None),
                    sv_len=-1,
                    sv_type_id=2,
                    sv_type_detail_id=None,
                    xpos=1000180928,
                    key_=0,
                ),
            ],
        )

    @patch(
        'v03_pipeline.lib.tasks.write_new_variants_table.load_gencode_gene_symbol_to_gene_id',
    )
    def test_sv_multiple_project_single_vcf(
        self,
        mock_load_gencode: Mock,
    ) -> None:
        mock_load_gencode.return_value = GENE_ID_MAPPING
        worker = luigi.worker.Worker()
        update_variant_annotations_task = (
            UpdateVariantAnnotationsTableWithNewSamplesTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.SV,
                sample_type=SampleType.WGS,
                callset_path=TEST_SV_VCF_2,
                project_guids=['R0116_test_project3', 'R0117_test_project4'],
                project_pedigree_paths=[TEST_PEDIGREE_10, TEST_PEDIGREE_11],
                skip_validation=True,
                run_id='run_id',
            )
        )
        worker.add(update_variant_annotations_task)
        worker.run()
        self.assertTrue(update_variant_annotations_task.complete())
        ht = hl.read_table(update_variant_annotations_task.output().path)
        self.assertEqual(ht.count(), 2)
        self.assertEqual(
            len(ht.globals.updates.collect()[0]),
            2,
        )
        self.assertEqual(
            ht.gt_stats.collect(),
            [
                hl.Struct(AF=0.25, AC=1, AN=4, Hom=0, Het=1),
                # Note the call stats computed
                # from the two included samples.
                hl.Struct(AF=0.0, AC=0, AN=4, Hom=0, Het=0),
            ],
        )

    def test_gcnv_update_vat_multiple(
        self,
    ) -> None:
        worker = luigi.worker.Worker()
        update_variant_annotations_task = (
            UpdateVariantAnnotationsTableWithNewSamplesTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.GCNV,
                sample_type=SampleType.WES,
                callset_path=TEST_GCNV_BED_FILE,
                project_guids=['R0115_test_project2'],
                project_pedigree_paths=[TEST_PEDIGREE_5],
                skip_validation=True,
                run_id=TEST_RUN_ID,
            )
        )
        worker.add(update_variant_annotations_task)
        worker.run()
        self.assertTrue(update_variant_annotations_task.complete())
        self.assertFalse(
            GCSorLocalFolderTarget(
                f'{self.mock_env.REFERENCE_DATASETS_DIR}/v03/GRCh38/GCNV/lookup.ht',
            ).exists(),
        )
        ht = hl.read_table(update_variant_annotations_task.output().path)
        self.assertEqual(ht.count(), 2)
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    versions=hl.Struct(),
                    enums=hl.Struct(
                        sv_type=SV_TYPES,
                        sorted_gene_consequences=hl.Struct(
                            major_consequence=SV_CONSEQUENCE_RANKS,
                        ),
                    ),
                    migrations=[],
                    max_key_=1,
                    updates={
                        hl.Struct(
                            callset=TEST_GCNV_BED_FILE,
                            project_guid='R0115_test_project2',
                            remap_pedigree_hash=hl.eval(
                                remap_pedigree_hash(
                                    TEST_PEDIGREE_5,
                                ),
                            ),
                        ),
                    },
                ),
            ],
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    variant_id='suffix_16456_DEL',
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=100023213,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=hl.eval(hl.float32(4.401408e-05)),
                        AC=1,
                        AN=22720,
                        Hom=None,
                        Het=None,
                    ),
                    num_exon=3,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=100472493,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=100488769,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000117620', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG00000283761', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG22222222222', major_consequence_id=None),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=100006937,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=hl.eval(hl.float32(0.583))),
                    sv_type_id=5,
                    xpos=1100006937,
                    key_=0,
                ),
                hl.Struct(
                    variant_id='suffix_16457_DEL',
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=100023212,
                        reference_genome='GRCh38',
                    ),
                    gt_stats=hl.Struct(
                        AF=8.802817319519818e-05,
                        AC=2,
                        AN=22719,
                        Hom=None,
                        Het=None,
                    ),
                    num_exon=2,
                    rg37_locus=hl.Locus(
                        contig=1,
                        position=100483142,
                        reference_genome='GRCh37',
                    ),
                    rg37_locus_end=hl.Locus(
                        contig=1,
                        position=100488768,
                        reference_genome='GRCh37',
                    ),
                    sorted_gene_consequences=[
                        hl.Struct(gene_id='ENSG00000283761', major_consequence_id=0),
                        hl.Struct(gene_id='ENSG22222222222', major_consequence_id=None),
                    ],
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=100017586,
                        reference_genome='GRCh38',
                    ),
                    strvctvre=hl.Struct(score=0.5070000290870667),
                    sv_type_id=5,
                    xpos=1100017586,
                    key_=1,
                ),
            ],
        )
        update_variant_annotations_task = (
            UpdateVariantAnnotationsTableWithNewSamplesTask(
                reference_genome=ReferenceGenome.GRCh38,
                dataset_type=DatasetType.GCNV,
                sample_type=SampleType.WES,
                callset_path=TEST_GCNV_BED_FILE_2,
                project_guids=['R0115_test_project2'],
                project_pedigree_paths=[TEST_PEDIGREE_8],
                skip_validation=True,
                run_id='second_run_id',
            )
        )
        worker.add(update_variant_annotations_task)
        worker.run()
        ht = hl.read_table(update_variant_annotations_task.output().path)
        self.assertEqual(ht.count(), 3)
        self.assertCountEqual(
            ht.select('gt_stats').collect(),
            [
                hl.Struct(
                    # Existing variant, gt_stats are preserved
                    variant_id='suffix_16456_DEL',
                    gt_stats=hl.Struct(
                        AF=hl.eval(hl.float32(4.401408e-05)),
                        AC=1,
                        AN=22720,
                        Hom=None,
                        Het=None,
                    ),
                ),
                hl.Struct(
                    # Exiting variant, gt_stats are updated
                    variant_id='suffix_16457_DEL',
                    gt_stats=hl.Struct(
                        AF=8.111110946629196e-05,
                        AC=3,
                        AN=36986,
                        Hom=None,
                        Het=None,
                    ),
                ),
                hl.Struct(
                    # New variant.
                    variant_id='suffix_99999_DEL',
                    gt_stats=hl.Struct(
                        AF=8.802817319519818e-05,
                        AC=2,
                        AN=22719,
                        Hom=None,
                        Het=None,
                    ),
                ),
            ],
        )
