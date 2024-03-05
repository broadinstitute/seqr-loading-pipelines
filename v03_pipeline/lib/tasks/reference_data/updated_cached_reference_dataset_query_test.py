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
    SampleType,
)
from v03_pipeline.lib.paths import (
    valid_cached_reference_dataset_query_path,
    valid_reference_dataset_collection_path,
)
from v03_pipeline.lib.reference_data.clinvar import CLINVAR_ASSERTIONS
from v03_pipeline.lib.reference_data.compare_globals import Globals
from v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query import (
    UpdatedCachedReferenceDatasetQuery,
)
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

COMBINED_1_PATH = 'v03_pipeline/var/test/reference_data/test_combined_1.ht'
CLINVAR_CRDQ_PATH = (
    'v03_pipeline/var/test/reference_data/test_clinvar_path_variants_crdq.ht'
)


class UpdatedCachedReferenceDatasetQueryTest(MockedDatarootTestCase):
    @mock.patch(
        'v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query.import_ht_from_config_path',
    )
    @mock.patch(
        'v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query.Globals',
    )
    @mock.patch(
        'v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query.HailTableTask',
    )
    def test_gnomad_qc(
        self,
        mock_hailtabletask,
        mock_globals_class,
        mock_import_raw_dataset,
    ) -> None:
        """
        Given a crdq task for gnomad_qc, expect the crdq table to be created by querying the raw dataset.
        """
        # raw dataset dependency exists
        mock_hailtabletask.return_value = MockCompleteTask()

        # import_ht_from_config_path returns a mock hail table for gnomad_qc
        mock_ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['A', 'C'],
                    'af': 0.00823,
                },
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
                af=hl.tfloat64,
            ),
            key=['locus', 'alleles'],
            globals=hl.Struct(
                path='gs://gnomad/sample_qc/mt/genomes_v3.1/gnomad_v3.1_qc_mt_v2_sites_dense.mt',
                enums=hl.Struct(),
                version='v3.1',
            ),
        )
        mock_import_raw_dataset.return_value = mock_ht

        # The first complete() call should return False by super.complete() because the crdq does not exist yet.
        # For the second complete() call, the crdq ht globals should be the same as dataset config
        mock_globals = Globals(
            paths={
                'gnomad_qc': 'gs://gnomad/sample_qc/mt/genomes_v3.1/gnomad_v3.1_qc_mt_v2_sites_dense.mt',
            },
            versions={'gnomad_qc': 'v3.1'},
            enums={},
            selects={'gnomad_qc': {'af'}},
        )
        mock_globals_class.from_ht.return_value = mock_globals
        mock_globals_class.from_dataset_configs.return_value = mock_globals

        worker = luigi.worker.Worker()
        task = UpdatedCachedReferenceDatasetQuery(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
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
                    paths=hl.Struct(
                        gnomad_qc='gs://gnomad/sample_qc/mt/genomes_v3.1/gnomad_v3.1_qc_mt_v2_sites_dense.mt',
                    ),
                    versions=hl.Struct(gnomad_qc='v3.1'),
                    enums=hl.Struct(gnomad_qc=hl.Struct()),
                ),
            ],
        )

    @mock.patch(
        'v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query.Globals.from_dataset_configs',
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
        mock_globals_from_dataset_configs,
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
            valid_cached_reference_dataset_query_path(
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

        # mock the clinvar_path_variants query
        def _clinvar_path_variants(table, **_: Any):
            table = table.select_globals()
            return table.select(
                pathogenic=False,
                likely_pathogenic=True,
            )

        mock_crdq_query.side_effect = _clinvar_path_variants

        dataset_globals = Globals(
            paths={
                'clinvar': 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/clinvar.vcf.gz',
            },
            versions={'clinvar': '2023-11-26'},  # matches rdc version
            enums={
                'clinvar': {
                    'pathogenicity': CLINVAR_PATHOGENICITIES,
                    'assertion': CLINVAR_ASSERTIONS,
                },
            },
            selects={
                'clinvar': {
                    'conflictingPathogenicities',
                    'pathogenicity_id',
                    'alleleId',
                    'goldStars',
                    'assertion_ids',
                },
            },
        )
        mock_globals_from_dataset_configs.return_value = dataset_globals

        worker = luigi.worker.Worker()
        task = UpdatedCachedReferenceDatasetQuery(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
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
                    pathogenic=False,
                    likely_pathogenic=True,
                ),
            ],
        )
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    paths=hl.Struct(
                        clinvar='ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/clinvar.vcf.gz',
                    ),
                    enums=hl.Struct(
                        clinvar=hl.Struct(
                            pathogenicity=CLINVAR_PATHOGENICITIES,
                            assertion=CLINVAR_ASSERTIONS,
                        ),
                    ),
                    versions=hl.Struct(
                        clinvar='2023-11-26',  # crdq table should have new versionr
                    ),
                ),
            ],
        )
