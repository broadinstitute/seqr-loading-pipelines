import shutil
from typing import Any
from unittest import mock

import hail as hl
import luigi

from v03_pipeline.lib.annotations.enums import CLINVAR_PATHOGENICITIES
from v03_pipeline.lib.model import (
    CachedReferenceDatasetQuery,
    DatasetType,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import (
    cached_reference_dataset_query_path,
    valid_reference_dataset_collection_path,
)
from v03_pipeline.lib.reference_data.clinvar import CLINVAR_ASSERTIONS
from v03_pipeline.lib.reference_data.config import CONFIG
from v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query import (
    UpdatedCachedReferenceDatasetQuery,
)
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

COMBINED_1_PATH = 'v03_pipeline/var/test/reference_data/test_combined_1.ht'
CLINVAR_CRDQ_PATH = (
    'v03_pipeline/var/test/reference_data/test_clinvar_path_variants_crdq.ht'
)

MOCK_CONFIG = {
    'gnomad_qc': {
        '38': {
            'version': 'v3.1',
            'source_path': 'gs://gnomad/sample_qc/mt/genomes_v3.1/gnomad_v3.1_qc_mt_v2_sites_dense.mt',
            'custom_import': lambda *_: hl.Table.parallelize(
                [
                    {
                        'locus': hl.Locus(
                            contig='chr1',
                            position=871269,
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
                globals=hl.Struct(),
            ),
        },
    },
    'clinvar': {
        '38': {
            **CONFIG['clinvar']['38'],
            'source_path': 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/clinvar.vcf.gz',
            'custom_import': lambda *_: hl.Table.parallelize(
                [],
                hl.tstruct(
                    locus=hl.tlocus('GRCh38'),
                    alleles=hl.tarray(hl.tstr),
                    info=hl.tstruct(
                        ALLELEID=hl.tint32,
                        CLNSIG=hl.tarray(hl.tstr),
                        CLNSIGCONF=hl.tarray(hl.tstr),
                        CLNREVSTAT=hl.tarray(hl.tstr),
                    ),
                    submitters=hl.tarray(hl.tstr),
                    conditions=hl.tarray(hl.tstr),
                ),
                key=['locus', 'alleles'],
                globals=hl.Struct(
                    version='2023-11-26',
                ),
            ),
        },
    },
}


class UpdatedCachedReferenceDatasetQueryTest(MockedDatarootTestCase):
    @mock.patch.dict(
        'v03_pipeline.lib.reference_data.compare_globals.CONFIG',
        MOCK_CONFIG,
    )
    @mock.patch(
        'v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query.CONFIG',
        MOCK_CONFIG,
    )
    @mock.patch(
        'v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query.HailTableTask',
    )
    def test_gnomad_qc(
        self,
        mock_hailtabletask,
    ) -> None:
        """
        Given a crdq task for gnomad_qc, expect the crdq table to be created by querying the raw dataset.
        """
        # raw dataset dependency exists
        mock_hailtabletask.return_value = MockCompleteTask()

        worker = luigi.worker.Worker()
        task = UpdatedCachedReferenceDatasetQuery(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            crdq=CachedReferenceDatasetQuery.GNOMAD_QC,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())

        ht = hl.read_table(task.output().path)
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
                ),
            ],
        )
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    paths=hl.Struct(gnomad_qc=CONFIG['gnomad_qc']['38']['source_path']),
                    versions=hl.Struct(gnomad_qc='v3.1'),
                    enums=hl.Struct(gnomad_qc=hl.Struct()),
                ),
            ],
        )

    @mock.patch.dict(
        'v03_pipeline.lib.reference_data.compare_globals.CONFIG',
        MOCK_CONFIG,
    )
    @mock.patch(
        'v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query.UpdatedReferenceDatasetCollectionTask',
    )
    @mock.patch(
        'v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query.CachedReferenceDatasetQuery.query',
    )
    def test_clinvar(
        self,
        mock_crdq_query,
        mock_updated_rdc_task,
    ) -> None:
        """
        Given a crdq task where there exists a clinvar crdq table and a clinvar rdc table,
        expect task to replace the clinvar crdq table with new version.
        """
        # rdc dependency exists
        mock_updated_rdc_task.return_value = MockCompleteTask()

        # copy existing crdq to test path
        # clinvar has version '2022-01-01'
        shutil.copytree(
            CLINVAR_CRDQ_PATH,
            cached_reference_dataset_query_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                CachedReferenceDatasetQuery.CLINVAR_PATH_VARIANTS,
            ),
        )

        # copy existing rdc to test path
        # clinvar has version '2023-11-26'
        shutil.copytree(
            COMBINED_1_PATH,
            valid_reference_dataset_collection_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ReferenceDatasetCollection.COMBINED,
            ),
        )

        # mock the clinvar_path_variants query to something simpler for testing
        def _clinvar_path_variants(table, **_: Any):
            table = table.select_globals()
            return table.select(
                is_pathogenic=False,
                is_likely_pathogenic=True,
            )

        mock_crdq_query.side_effect = _clinvar_path_variants

        worker = luigi.worker.Worker()
        task = UpdatedCachedReferenceDatasetQuery(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            crdq=CachedReferenceDatasetQuery.CLINVAR_PATH_VARIANTS,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())

        ht = hl.read_table(task.output().path)
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
                    is_pathogenic=False,
                    is_likely_pathogenic=True,
                ),
            ],
        )
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    paths=hl.Struct(
                        clinvar=MOCK_CONFIG['clinvar']['38']['source_path'],
                    ),
                    enums=hl.Struct(
                        clinvar=hl.Struct(
                            pathogenicity=CLINVAR_PATHOGENICITIES,
                            assertion=CLINVAR_ASSERTIONS,
                        ),
                    ),
                    versions=hl.Struct(
                        clinvar='2023-11-26',  # crdq table should have new clinvar version
                    ),
                ),
            ],
        )
