import unittest

import hail as hl

from v03_pipeline.lib.annotations import sv
from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.model import DatasetType


class SVTest(unittest.TestCase):
    def test_sv_export_annotations(self) -> None:
        ht = hl.Table.parallelize(
            [
                hl.Struct(
                    id=0,
                    algorithms='manta',
                    end_locus=hl.Locus(
                        contig='chr5',
                        position=20404,
                        reference_genome='GRCh38',
                    ),
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=180928,
                        reference_genome='GRCh38',
                    ),
                    sv_len=123,
                    sv_type_id=2,
                    sv_type_detail_id=None,
                ),
                hl.Struct(
                    id=1,
                    algorithms='manta',
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=789481,
                        reference_genome='GRCh38',
                    ),
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=789481,
                        reference_genome='GRCh38',
                    ),
                    sv_len=245,
                    sv_type_id=2,
                    sv_type_detail_id=None,
                ),
                hl.Struct(
                    id=2,
                    algorithms='manta',
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=6559723,
                        reference_genome='GRCh38',
                    ),
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=6558902,
                        reference_genome='GRCh38',
                    ),
                    sv_len=245,
                    sv_type_id=3,
                    sv_type_detail_id=2,
                ),
                hl.Struct(
                    id=3,
                    algorithms='manta',
                    end_locus=hl.Locus(
                        contig='chr1',
                        position=6559723,
                        reference_genome='GRCh38',
                    ),
                    start_locus=hl.Locus(
                        contig='chr1',
                        position=6558902,
                        reference_genome='GRCh38',
                    ),
                    sv_len=245,
                    sv_type_id=7,
                    sv_type_detail_id=6,
                ),
            ],
            hl.tstruct(
                id=hl.tint32,
                algorithms=hl.tstr,
                end_locus=hl.tlocus('GRCh38'),
                start_locus=hl.tlocus('GRCh38'),
                sv_len=hl.tint32,
                sv_type_id=hl.tint32,
                sv_type_detail_id=hl.tint32,
            ),
            key='id',
        )
        ht = ht.select(
            **get_fields(
                ht,
                DatasetType.SV.export_vcf_annotation_fns,
            ),
        )
        self.assertEqual(
            ht.collect(),
            [
                hl.Struct(
                    id=0,
                    locus=hl.Locus(
                        contig='chr1',
                        position=180928,
                        reference_genome='GRCh38',
                    ),
                    alleles=['N', '<BND>'],
                    info=hl.Struct(
                        ALGORITHMS='manta',
                        END=180928,
                        CHR2='chr5',
                        END2=20404,
                        SVTYPE='BND',
                        SVLEN=123,
                    ),
                ),
                hl.Struct(
                    id=1,
                    locus=hl.Locus(
                        contig='chr1',
                        position=789481,
                        reference_genome='GRCh38',
                    ),
                    alleles=['N', '<BND>'],
                    info=hl.Struct(
                        ALGORITHMS='manta',
                        END=789481,
                        CHR2='chr1',
                        END2=789481,
                        SVTYPE='BND',
                        SVLEN=245,
                    ),
                ),
                hl.Struct(
                    id=2,
                    locus=hl.Locus(
                        contig='chr1',
                        position=6558902,
                        reference_genome='GRCh38',
                    ),
                    alleles=['N', '<CPX>'],
                    info=hl.Struct(
                        ALGORITHMS='manta',
                        END=6558902,
                        CHR2='chr1',
                        END2=6559723,
                        SVTYPE='CPX',
                        SVLEN=245,
                    ),
                ),
                hl.Struct(
                    id=3,
                    locus=hl.Locus(
                        contig='chr1',
                        position=6558902,
                        reference_genome='GRCh38',
                    ),
                    alleles=['N', '<INS:ME:SVA>'],
                    info=hl.Struct(
                        ALGORITHMS='manta',
                        END=6558902,
                        CHR2='chr1',
                        END2=6559723,
                        SVTYPE='INS',
                        SVLEN=245,
                    ),
                ),
            ],
        )

    def test_allele_count_annotations(self) -> None:
        ht = hl.Table.parallelize(
            [
                {
                    'variant_id': 0,
                    'gt_stats': hl.Struct(
                        AC=4,
                        AN=8,
                        AF=hl.float32(0.5),
                        Hom=1,
                        Het=2,
                    ),
                },
                {'variant_id': 1, 'gt_stats': None},
            ],
            hl.tstruct(
                variant_id=hl.tint32,
                gt_stats=hl.tstruct(
                    AC=hl.tint32,
                    AN=hl.tint32,
                    AF=hl.tfloat32,
                    Hom=hl.tint32,
                    Het=hl.tint32,
                ),
            ),
            key='variant_id',
        )
        callset_ht = hl.Table.parallelize(
            [
                {
                    'variant_id': 0,
                    'gt_stats': hl.Struct(
                        AC=[0, 3],
                        AN=6,
                        homozygote_count=[0, 1],
                    ),
                },
                {
                    'variant_id': 2,
                    'gt_stats': hl.Struct(
                        AC=[0, 2],
                        AN=6,
                        homozygote_count=[0, 1],
                    ),
                },
            ],
            hl.tstruct(
                variant_id=hl.tint32,
                gt_stats=hl.tstruct(
                    AC=hl.tarray(hl.tint32),
                    AN=hl.tint32,
                    homozygote_count=hl.tarray(hl.tint32),
                ),
            ),
            key='variant_id',
        )
        ht = ht.select(gt_stats=sv.gt_stats(ht, callset_ht))
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    variant_id=0,
                    gt_stats=hl.Struct(AC=7, AN=14, AF=0.5, Hom=2, Het=3),
                ),
                hl.Struct(
                    variant_id=1,
                    gt_stats=hl.Struct(AC=None, AN=None, AF=None, Hom=None, Het=None),
                ),
            ],
        )
