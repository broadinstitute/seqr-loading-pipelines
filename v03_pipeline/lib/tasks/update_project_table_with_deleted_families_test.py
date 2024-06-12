import hail as hl
import luigi

from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.paths import project_table_path
from v03_pipeline.lib.tasks.update_project_table_with_deleted_families import (
    UpdateProjectTableWithDeletedFamiliesTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase


class UpdateProjectTableWithDeletedFamiliesTaskTest(MockedDatarootTestCase):
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

    def test_update_project_with_deleted_families(self):
        worker = luigi.worker.Worker()
        task = UpdateProjectTableWithDeletedFamiliesTask(
            dataset_type=DatasetType.SNV_INDEL,
            reference_genome=ReferenceGenome.GRCh38,
            project_guid='project_a',
            family_guids=['family_b'],
        )
        worker.add(task)
        worker.run()
        ht = hl.read_table(task.output().path)
        self.assertEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    family_guids=['family_a'],
                    family_samples={'family_a': ['1', '2', '3']},
                    sample_type='WGS',
                    updates={'v03_pipeline/var/test/callsets/1kg_30variants.vcf'},
                ),
            ],
        )
        self.assertEqual(len(ht.family_entries.collect()[0]), 1)
        self.assertEqual(len(ht.family_entries.collect()[0][0]), 3)
