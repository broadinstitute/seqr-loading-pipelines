import hail as hl
import logging

from sv_pipeline.load_data import get_sample_subset
from genome_sv_pipeline.load_data import GENES_COLUMNS, GENES_FIELD

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def sub_setting_mt(guid, mt):
    sample_subset = get_sample_subset(guid, 'WGS')
    logger.info('Total {} samples in project {}'.format(len(sample_subset), guid))
    mt1 = mt.filter_cols(hl.literal(sample_subset).contains(mt['s']))

    missing_samples = sample_subset - {col.s for col in mt1.key_cols_by().cols().collect()}
    logger.info('{} missing samples: {}'.format(len(missing_samples), missing_samples))

    samples = hl.agg.filter(mt1.GT.is_non_ref(), hl.agg.collect(hl.struct(id=mt1.s, gq=mt1.GQ, cn=mt1.RD_CN)))
    mt2 = mt1.annotate_rows(samples=samples)
    mt3 = mt2.filter_rows(mt2.samples != hl.empty_array(hl.dtype('struct{id: str, gq: int32, cn: int32}')))
    return mt3.rows()


def annotate_fields(rows):
    kwargs = {
        'contig': hl.int(rows.locus.contig.split('chr')[1]),
        'sc': rows.info.AC,
        'sf': rows.info.AF,
        'sn': rows.info.AN,
        'svDetail': hl.if_else(hl.is_defined(rows.info.CPX_TYPE), rows.info.CPX_TYPE, rows.info.SVTYPE),
        'start': rows.locus.position,
        'end': rows.info.END,
        'sv_callset_Hemi': rows.info.N_HET,
        'sv_callset_Hom': rows.info.N_HOMALT,
        'gnomad_svs_ID': rows.info.gnomAD_V2_SVID,
        'gnomad_svs_AF': rows.info.gnomAD_V2_AF,
        'chr2': rows.info.CHR2,
        'end2': rows.info.END2,
        GENES_FIELD: hl.flatten(
            [hl.if_else(hl.is_defined(rows.info[field_name]), rows.info[field_name], hl.empty_array('str'))
             for field_name in GENES_COLUMNS]),
    }
    rows = rows.annotate(**kwargs)

    mapping = {
        'rsid': 'variantId',
        'alleles': 'svType',
    }
    rows = rows.rename(mapping)

    fields = list(mapping.values()) + ['filters'] + list(kwargs.keys()) + ['samples']
    return rows.key_by('locus').select(*fields)


def main():
    hl.init()
    # hl.import_vcf('vcf/sv.vcf.gz', force=True, reference_genome='GRCh38').write('vcf/svs.mt', overwrite=True)
    mt = hl.read_matrix_table('vcf/svs.mt')
    rows = sub_setting_mt('R0332_cmg_estonia_wgs', mt)
    logger.info('Variant counts: {}'.format(rows.count()))

    rows = annotate_fields(rows)
    rows.show(width=200)


if __name__ == '__main__':
    main()

# Outputs:
# INFO:__main__:Total 167 samples in project R0332_cmg_estonia_wgs
# INFO:__main__:61 missing samples: {'OUN_HK124_002_D1', 'HK108-001_1', 'HK112-002_1', 'OUN_HK124_003_D1', 'OUN_HK124_001_D1', 'OUN_HK126_001_D1', 'HK104-002_D2', 'HK117-001_1', 'HK112-003_1', 'HK017-0046', 'HK085-004_D2', 'OUN_HK126_002_D1', 'OUN_HK132_002_D1', 'E00859946', 'HK081-003_D2', 'HK085-006_D2', 'HK115-001_1', 'OUN_HK132_003_D1', 'HK032_0081_2_D2', 'HK115-002_1', 'OUN_HK132_001_D1', 'HK060-0155_1', 'HK060-0156_1', 'HK117-002_1', 'OUN_HK131_003_D1', 'HK112-001_1', 'HK079-002_D2', 'HK100-004_D1', 'HK119-003_1', 'HK080-002_D2', 'HK032_0081', 'HK080-003_D2', 'HK061-0158_D1', 'HK080-001_D2', 'HK100-002_D1', 'OUN_HK126_003_D1', 'HK081-001_D2', 'HK017-0045', 'HK100-003_D1', 'HK104-001_D2', 'HK119-002_1', 'HK085-002_D2', 'HK060-0154_1', 'HK108-002_1', 'HK115-003_1', 'HK035_0089', 'HK100-001_D1', 'HK108-003_1', 'HK079-003_D2', 'OUN_HK131_001_D1', 'HK015_0036', 'HK117-003_1', 'HK079-001_D2', 'HK015_0038_D2', 'HK061-0157_D1', 'HK061-0159_D1', 'HK081-002_D2', 'HK085-001_D2', 'HK017-0044', 'HK119-001_1', 'OUN_HK131_002_D1'}
# [Stage 0:>                                                          (0 + 1) / 1]INFO:__main__:Variant counts: 67275
# +---------------+-----------------------------+---------------+------------------------------------------+--------+--------------+----------------+-------+----------+-------+--------+
# | locus         | variantId                   | svType        | filters                                  | contig | sc           | sf             |    sn | svDetail | start |    end |
# +---------------+-----------------------------+---------------+------------------------------------------+--------+--------------+----------------+-------+----------+-------+--------+
# | locus<GRCh38> | str                         | array<str>    | set<str>                                 |  int32 | array<int32> | array<float64> | int32 | str      | int32 |  int32 |
# +---------------+-----------------------------+---------------+------------------------------------------+--------+--------------+----------------+-------+----------+-------+--------+
# | chr1:10000    | "CMG.phase1_CMG_DUP_chr1_1" | ["N","<DUP>"] | {"LOW_CALL_RATE"}                        |      1 | [370]        | [2.59e-01]     |  1428 | "DUP"    | 10000 |  17000 |
# | chr1:10000    | "CMG.phase1_CMG_DUP_chr1_2" | ["N","<DUP>"] | {"LOW_CALL_RATE"}                        |      1 | [70]         | [4.90e-02]     |  1428 | "DUP"    | 10000 |  53500 |
# | chr1:10602    | "CMG.phase1_CMG_BND_chr1_1" | ["N","<BND>"] | {"UNRESOLVED","UNSTABLE_AF_PCRMINUS"}    |      1 | [88]         | [6.16e-02]     |  1428 | "BND"    | 10602 |  10602 |
# | chr1:41950    | "CMG.phase1_CMG_DUP_chr1_3" | ["N","<DUP>"] | {"LOW_CALL_RATE"}                        |      1 | [28]         | [1.96e-02]     |  1428 | "DUP"    | 41950 |  52000 |
# | chr1:44000    | "CMG.phase1_CMG_DUP_chr1_4" | ["N","<DUP>"] | {"UNSTABLE_AF_PCRMINUS","LOW_CALL_RATE"} |      1 | [96]         | [6.72e-02]     |  1428 | "DUP"    | 44000 |  66000 |
# | chr1:44250    | "CMG.phase1_CMG_DUP_chr1_5" | ["N","<DUP>"] | {"LOW_CALL_RATE"}                        |      1 | [82]         | [5.74e-02]     |  1428 | "DUP"    | 44250 | 116000 |
# | chr1:51400    | "CMG.phase1_CMG_DEL_chr1_1" | ["N","<DEL>"] | {"LOW_CALL_RATE"}                        |      1 | [306]        | [2.14e-01]     |  1428 | "DEL"    | 51400 |  64000 |
# | chr1:66234    | "CMG.phase1_CMG_BND_chr1_2" | ["N","<BND>"] | {"UNRESOLVED"}                           |      1 | [236]        | [1.65e-01]     |  1428 | "BND"    | 66234 |  66234 |
# | chr1:66350    | "CMG.phase1_CMG_DEL_chr1_2" | ["N","<DEL>"] | {"FAIL_OUTLIER_REMOVAL"}                 |      1 | [2]          | [1.40e-03]     |  1428 | "DEL"    | 66350 |  66427 |
# | chr1:66531    | "CMG.phase1_CMG_INS_chr1_1" | ["N","<INS>"] | {}                                       |      1 | [18]         | [1.26e-02]     |  1428 | "INS"    | 66531 |  66576 |
# +---------------+-----------------------------+---------------+------------------------------------------+--------+--------------+----------------+-------+----------+-------+--------+
#
# +-----------------+----------------+--------------------------+---------------+---------+--------+-------------------------------------+
# | sv_callset_Hemi | sv_callset_Hom | gnomad_svs_ID            | gnomad_svs_AF | chr2    |   end2 | geneIds                             |
# +-----------------+----------------+--------------------------+---------------+---------+--------+-------------------------------------+
# |           int32 |          int32 | str                      |       float64 | str     |  int32 | array<str>                          |
# +-----------------+----------------+--------------------------+---------------+---------+--------+-------------------------------------+
# |             228 |             71 | NA                       |            NA | "chr1"  |     NA | []                                  |
# |              60 |              5 | NA                       |            NA | "chr1"  |     NA | ["FAM138A","MIR1302-2HG"]           |
# |              88 |              0 | "gnomAD-SV_v2.1_BND_1_1" |      6.79e-03 | "chr12" |  10546 | []                                  |
# |              26 |              1 | "gnomAD-SV_v2.1_DUP_1_1" |      6.90e-02 | "chr1"  |     NA | []                                  |
# |              50 |             23 | NA                       |            NA | "chr1"  |     NA | ["OR4F5"]                           |
# |              54 |             14 | NA                       |            NA | "chr1"  |     NA | ["OR4F5","AL627309.3","AL627309.1"] |
# |             236 |             35 | NA                       |            NA | "chr1"  |     NA | []                                  |
# |             164 |             36 | NA                       |            NA | "chr19" | 108051 | []                                  |
# |               2 |              0 | "gnomAD-SV_v2.1_DEL_1_4" |      5.55e-04 | "chr1"  |     NA | ["OR4F5"]                           |
# |              18 |              0 | "gnomAD-SV_v2.1_INS_1_3" |      2.34e-04 | "chr1"  |     NA | ["OR4F5"]                           |
# +-----------------+----------------+--------------------------+---------------+---------+--------+-------------------------------------+
#
# +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# | samples                                                                                                                                                                                              |
# +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# | array<struct{id: str, gq: int32, cn: int32}>                                                                                                                                                         |
# +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# | [("HK010_0026",145,3),("HK015_0037",37,3),("HK031_0079",999,3),("HK031_0080",37,4),("HK069-0178_1",104,3),("HK072-001_1",999,3),("HK072-003_1",999,3),("HK073-001_1",110,3),("HK075-001_1",999,3)... |
# | [("HK015_0037",999,4),("HK031_0080",104,3),("HK104-003_1",36,3),("HK088-003_1",999,3),("HK102-001_1",141,3),("OUN_HK120_2_1",999,3),("HK025-0068_3",999,3),("HK029_0076",999,3)]                     |
# | [("HK072-001_1",999,NA),("HK102-002_1",1,NA),("HK104-003_1",1,NA),("HK047_0116",18,NA),("HK088-003_1",523,NA),("HK095-001_1",1,NA),("OUN_HK120_2_1",1,NA),("OUN_HK120_3_1",1,NA),("HK008_0022_3",... |
# | [("HK090-003_1",116,3),("HK029-0076_2",999,3),("HK036_0091",999,3),("HK036_0093",999,3)]                                                                                                             |
# | [("HK031_0080",36,3),("HK088-003_1",61,3),("HK003_0007",999,3),("HK003_0008",999,3),("HK029-0076_2",999,3),("HK036_0091",92,3)]                                                                      |
# | [("HK088-003_1",999,3),("HK003_0007",999,3),("HK029-0076_2",999,3),("HK036_0091",114,3),("HK071-002",999,3)]                                                                                         |
# | [("HK015_0037",999,1),("HK069-0178_1",125,1),("HK072-002_1",999,0),("HK075-001_1",1,1),("HK075-003_1",156,0),("HK087-001_1",999,0),("HK022_0060",1,1),("HK031_0078",1,1),("HK047_0116",999,0),("H... |
# | [("HK010_0026",688,NA),("HK015_0037",76,NA),("HK031_0079",381,NA),("HK031_0080",76,NA),("HK003_0009",487,NA),("HK012_0031_2",306,NA),("HK022_0060",815,NA),("HK024_0067",90,NA),("HK031_0078",962... |
# | [("HK031_0080",99,7)]                                                                                                                                                                                |
# | [("HK015_0037",49,NA),("HK069-0178_1",1,NA),("HK053-0134_1",1,NA),("HK006_0018",1,NA),("HK008_0022",1,NA),("HK009_0023",1,NA),("HK009_0024",1,NA),("HK018_0048",1,NA)]                               |
# +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# showing top 10 rows
