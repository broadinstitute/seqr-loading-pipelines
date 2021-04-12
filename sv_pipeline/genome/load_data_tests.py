import unittest
import mock
import os
import tempfile

import hail as hl

from sv_pipeline.genome.load_data import sub_setting_mt

VCF_DATA = [
'##fileformat=VCFv4.2',
'##FORMAT=<ID=CN,Number=1,Type=Integer,Description="Predicted copy state">',
'##FORMAT=<ID=CNQ,Number=1,Type=Integer,Description="Read-depth genotype quality">',
'##FORMAT=<ID=EV,Number=1,Type=String,Description="Classes of evidence supporting final genotype">',
'##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">',
'##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">',
'##FORMAT=<ID=PE_GQ,Number=1,Type=Integer,Description="Paired-end genotype quality">',
'##FORMAT=<ID=PE_GT,Number=1,Type=Integer,Description="Paired-end genotype">',
'##FORMAT=<ID=RD_CN,Number=1,Type=Integer,Description="Predicted copy state">',
'##FORMAT=<ID=RD_GQ,Number=1,Type=Integer,Description="Read-depth genotype quality">',
'##FORMAT=<ID=SR_GQ,Number=1,Type=Integer,Description="Split read genotype quality">',
'##FORMAT=<ID=SR_GT,Number=1,Type=Integer,Description="Split-read genotype">',
'#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE-1	SAMPLE-2	SAMPLE-3	SAMPLE-4	SAMPLE-5',
'chr1	10000	CMG.phase1_CMG_DUP_chr1_1	N	<DUP>	999	LOW_CALL_RATE		GT:GQ:RD_CN:RD_GQ:PE_GT:PE_GQ:SR_GT:SR_GQ:EV	0/1:999:3:999:.:.:.:.:RD	0/1:52:3:52:.:.:.:.:RD	0/1:19:3:19:.:.:.:.:RD	0/0:1:2:1:.:.:.:.:RD	0/0:31:2:31:.:.:.:.:RD',
'chr1	10000	CMG.phase1_CMG_DUP_chr1_2	N	<DUP>	999	LOW_CALL_RATE		GT:GQ:RD_CN:RD_GQ:PE_GT:PE_GQ:SR_GT:SR_GQ:EV	0/0:1:2:1:.:.:.:.:RD	0/1:119:3:119:.:.:.:.:RD	0/1:119:3:119:.:.:.:.:RD	0/0:999:2:999:.:.:.:.:RD	0/0:133:2:133:.:.:.:.:RD',
'chr1	10602	CMG.phase1_CMG_BND_chr1_1	N	<BND>	461	UNRESOLVED;UNSTABLE_AF_PCRMINUS		GT:GQ:RD_CN:RD_GQ:PE_GT:PE_GQ:SR_GT:SR_GQ:EV	0/0:999:.:.:0:23:0:999:PE,SR	0/0:999:.:.:0:23:0:999:PE,SR	0/0:999:.:.:0:1:0:999:PE,SR	0/0:999:.:.:0:3:0:999:PE,SR	0/0:999:.:.:0:23:0:999:PE,SR',
'chr1	41950	CMG.phase1_CMG_DUP_chr1_3	N	<DUP>	999	LOW_CALL_RATE		GT:GQ:RD_CN:RD_GQ:PE_GT:PE_GQ:SR_GT:SR_GQ:EV	0/0:31:2:31:.:.:.:.:RD	0/0:58:2:58:.:.:.:.:RD	0/0:1:2:1:.:.:.:.:RD	0/0:112:2:112:.:.:.:.:RD	0/0:999:2:999:.:.:.:.:RD',
'chr1	44000	CMG.phase1_CMG_DUP_chr1_4	N	<DUP>	999	UNSTABLE_AF_PCRMINUS;LOW_CALL_RATE		GT:GQ:RD_CN:RD_GQ:PE_GT:PE_GQ:SR_GT:SR_GQ:EV	0/0:125:1:125:.:.:.:.:RD	0/0:72:2:72:.:.:.:.:RD	0/0:130:2:130:.:.:.:.:RD	0/0:1:2:1:.:.:.:.:RD	0/0:1:2:1:.:.:.:.:RD',
'chr1	44250	CMG.phase1_CMG_DUP_chr1_5	N	<DUP>	999	LOW_CALL_RATE		GT:GQ:RD_CN:RD_GQ:PE_GT:PE_GQ:SR_GT:SR_GQ:EV	0/0:1:1:1:.:.:.:.:RD	0/0:36:2:36:.:.:.:.:RD	0/0:94:2:94:.:.:.:.:RD	0/0:130:1:130:.:.:.:.:RD	0/0:999:1:999:.:.:.:.:RD',
'chr1	51400	CMG.phase1_CMG_DEL_chr1_1	N	<DEL>	999	LOW_CALL_RATE		GT:GQ:RD_CN:RD_GQ:PE_GT:PE_GQ:SR_GT:SR_GQ:EV	0/1:125:1:125:.:.:.:.:RD	0/0:72:2:72:.:.:.:.:RD	0/0:112:2:112:.:.:.:.:RD	0/0:1:2:1:.:.:.:.:RD	0/0:8:2:8:.:.:.:.:RD',
'chr1	52600	CMG.phase1_CMG_CNV_chr1_1	N	<CNV>	999	FAIL_minGQ		GT:GQ:RD_CN:RD_GQ:PE_GT:PE_GQ:SR_GT:SR_GQ:EV:CN:CNQ	.:.:1:125:.:.:.:.:RD:1:125	.:.:2:130:.:.:.:.:RD:2:130	.:.:2:23:.:.:.:.:RD:2:23	.:.:2:1:.:.:.:.:RD:2:1	.:.:2:1:.:.:.:.:RD:2:1',
'chr1	66234	CMG.phase1_CMG_BND_chr1_2	N	<BND>	807	UNRESOLVED		GT:GQ:RD_CN:RD_GQ:PE_GT:PE_GQ:SR_GT:SR_GQ:EV	0/0:999:.:.:0:23:0:999:PE,SR	0/0:999:.:.:0:999:0:999:PE,SR	0/0:999:.:.:0:999:0:999:PE,SR	0/0:999:.:.:0:999:0:999:PE,SR	0/0:999:.:.:0:999:0:999:PE,SR',
]


class LoadDataTest(unittest.TestCase):
    def setUp(self):
        self.vcf_file = tempfile.mkstemp(suffix='.vcf')[1]
        with open(self.vcf_file, 'w') as f:
            f.writelines('\n'.join(VCF_DATA))
        hl.init()
        self.mt = hl.import_vcf(self.vcf_file, reference_genome='GRCh38')

    def tearDown(self):
        os.remove(self.vcf_file)

    @mock.patch('sv_pipeline.genome.load_data.get_sample_subset')
    @mock.patch('sv_pipeline.genome.load_data.logger')
    def test_sub_setting_mt(self, mock_logger, mock_get_sample):
        mock_get_sample.return_value = {'SAMPLE-1', 'SAMPLE-3'}
        mt = sub_setting_mt('test_guid', self.mt)
        mock_logger.info.assert_called_with('No missing samples.')
        mock_get_sample.assert_called_with('test_guid', 'WGS')
        self.assertEqual(mt.count(), 3)
