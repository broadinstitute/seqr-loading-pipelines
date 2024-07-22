import pathlib

import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.paths import family_table_path, project_table_path
from v03_pipeline.lib.tasks.delete_project_family_tables import (
    DeleteProjectFamilyTablesTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase


class DeleteTableTaskTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=876499,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['A', 'G'],
                    'filters': set(),
                    'family_entries': [
                        [
                            hl.Struct(
                                GQ=21,
                                AB=1.0,
                                DP=7,
                                GT=hl.Call(alleles=[1, 1], phased=False),
                            ),
                            hl.Struct(
                                GQ=24,
                                AB=1.0,
                                DP=8,
                                GT=hl.Call(alleles=[1, 1], phased=False),
                            ),
                            hl.Struct(
                                GQ=12,
                                AB=1.0,
                                DP=4,
                                GT=hl.Call(alleles=[1, 1], phased=False),
                            ),
                        ],
                        [
                            hl.Struct(
                                GQ=61,
                                AB=hl.eval(hl.float32(0.6)),
                                DP=5,
                                GT=hl.Call(alleles=[0, 1], phased=False),
                            ),
                        ],
                    ],
                },
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=878314,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['G', 'C'],
                    'filters': set(),
                    'family_entries': [
                        [
                            hl.Struct(
                                GQ=30,
                                AB=hl.eval(hl.float32(0.3333333333333333)),
                                DP=3,
                                GT=hl.Call(alleles=[0, 1], phased=False),
                            ),
                            hl.Struct(
                                GQ=6,
                                AB=0.0,
                                DP=2,
                                GT=hl.Call(alleles=[0, 0], phased=False),
                            ),
                            hl.Struct(
                                GQ=61,
                                AB=hl.eval(hl.float32(0.6)),
                                DP=5,
                                GT=hl.Call(alleles=[0, 1], phased=False),
                            ),
                        ],
                        [
                            hl.Struct(
                                GQ=61,
                                AB=hl.eval(hl.float32(0.6)),
                                DP=5,
                                GT=hl.Call(alleles=[0, 1], phased=False),
                            ),
                        ],
                    ],
                },
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
                filters=hl.tset(hl.tstr),
                family_entries=hl.tarray(
                    hl.tarray(
                        hl.tstruct(
                            GQ=hl.tint32,
                            AB=hl.tfloat32,
                            DP=hl.tint32,
                            GT=hl.tcall,
                        ),
                    ),
                ),
            ),
            key=['locus', 'alleles'],
            globals=hl.Struct(
                family_guids=['family_a', 'family_b'],
                family_samples={'family_a': ['1', '2', '3'], 'family_b': ['4']},
                sample_type='WGS',
                updates={'v03_pipeline/var/test/callsets/1kg_30variants.vcf'},
            ),
        )
        ht.write(
            project_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                'project_a',
            ),
        )
        for family_guid in hl.eval(ht.globals.family_guids):
            family_ht = hl.utils.range_table(100)
            family_ht.write(
                family_table_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    SampleType.WGS,
                    family_guid,
                ),
            )

    def test_delete_project_family_tables_task(self) -> None:
        self.assertTrue(
            pathlib.Path(
                family_table_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    SampleType.WGS,
                    'family_a',
                ),
            ).exists(),
        )
        self.assertFalse(
            pathlib.Path(
                family_table_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    SampleType.WES,
                    'family_a',
                ),
            ).exists(),
        )
        worker = luigi.worker.Worker()
        task = DeleteProjectFamilyTablesTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            project_guid='project_a',
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
        for family_guid in ['family_a', 'family_b']:
            self.assertFalse(
                pathlib.Path(
                    family_table_path(
                        ReferenceGenome.GRCh38,
                        DatasetType.SNV_INDEL,
                        SampleType.WGS,
                        family_guid,
                    ),
                ).exists(),
            )
