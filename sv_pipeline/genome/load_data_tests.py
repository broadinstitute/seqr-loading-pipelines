import mock
import os
import sys
import tempfile
import unittest
import copy

import hail as hl

from sv_pipeline.genome.load_data import load_mt, subset_mt, export_to_es, main, WGS_SAMPLE_TYPE

GENE_ID_MAPPING = {
    'OR4F5': 'ENSG00000186092', 'PLEKHG4B': 'ENSG00000153404', 'OR4F16': 'ENSG00000186192',
    'OR4F29': 'ENSG00000284733', 'FBXO28': 'ENSG00000143756', 'SAMD11': 'ENSG00000187634',
    'C1orf174': 'ENSG00000198912', 'TAS1R1': 'ENSG00000173662', 'FAM131C': 'ENSG00000185519', 'RCC2': 'ENSG00000179051',
    'NBPF3': 'ENSG00000142794', 'AGBL4': 'ENSG00000186094', 'KIAA1614': 'ENSG00000135835', 'MR1': 'ENSG00000153029',
    'STX6': 'ENSG00000135823', 'XPR1': 'ENSG00000143324'
}

VCF_HEADER_META = [
'##fileformat=VCFv4.2',
'##FORMAT=<ID=CN,Number=1,Type=Integer,Description="Predicted copy state">',
'##FORMAT=<ID=CNQ,Number=1,Type=Integer,Description="Read-depth genotype quality">',
'##FORMAT=<ID=EV,Number=.,Type=String,Description="Classes of evidence supporting final genotype">',
'##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">',
'##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">',
'##FORMAT=<ID=PE_GQ,Number=1,Type=Integer,Description="Paired-end genotype quality">',
'##FORMAT=<ID=PE_GT,Number=1,Type=Integer,Description="Paired-end genotype">',
'##FORMAT=<ID=RD_CN,Number=1,Type=Integer,Description="Predicted copy state">',
'##FORMAT=<ID=RD_GQ,Number=1,Type=Integer,Description="Read-depth genotype quality">',
'##FORMAT=<ID=SR_GQ,Number=1,Type=Integer,Description="Split read genotype quality">',
'##FORMAT=<ID=SR_GT,Number=1,Type=Integer,Description="Split-read genotype">',
'##INFO=<ID=ALGORITHMS,Number=.,Type=String,Description="Source algorithms">',
'##INFO=<ID=CHR2,Number=1,Type=String,Description="Chromosome for END coordinate">',
'##INFO=<ID=CPX_INTERVALS,Number=.,Type=String,Description="Genomic intervals constituting complex variant.">',
'##INFO=<ID=CPX_TYPE,Number=1,Type=String,Description="Class of complex variant.">',
'##INFO=<ID=END,Number=1,Type=Integer,Description="End position of the structural variant">',
'##INFO=<ID=END2,Number=1,Type=Integer,Description="Position of breakpoint on CHR2">',
'##INFO=<ID=EVIDENCE,Number=.,Type=String,Description="Classes of random forest support.">',
'##INFO=<ID=SOURCE,Number=1,Type=String,Description="Source of inserted sequence.">',
'##INFO=<ID=STRANDS,Number=1,Type=String,Description="Breakpoint strandedness [++,+-,-+,--]">',
'##INFO=<ID=SVLEN,Number=1,Type=Integer,Description="SV length">',
'##INFO=<ID=SVTYPE,Number=1,Type=String,Description="Type of structural variant">',
'##INFO=<ID=UNRESOLVED_TYPE,Number=1,Type=String,Description="Class of unresolved variant.">',
'##INFO=<ID=PREDICTED_BREAKEND_EXONIC,Number=.,Type=String,Description="Gene(s) for which the SV breakend is predicted to fall in an exon.">',
'##INFO=<ID=PREDICTED_COPY_GAIN,Number=.,Type=String,Description="Gene(s) on which the SV is predicted to have a copy-gain effect.">',
'##INFO=<ID=PREDICTED_DUP_PARTIAL,Number=.,Type=String,Description="Gene(s) which are partially overlapped by an SV\'s duplication, but the transcription start site is not duplicated.">',
'##INFO=<ID=PREDICTED_INTERGENIC,Number=0,Type=Flag,Description="SV does not overlap any protein-coding genes.">',
'##INFO=<ID=PREDICTED_INTRAGENIC_EXON_DUP,Number=.,Type=String,Description="Gene(s) on which the SV is predicted to result in intragenic exonic duplication without breaking any coding sequences.">',
'##INFO=<ID=PREDICTED_INTRONIC,Number=.,Type=String,Description="Gene(s) where the SV was found to lie entirely within an intron.">',
'##INFO=<ID=PREDICTED_INV_SPAN,Number=.,Type=String,Description="Gene(s) which are entirely spanned by an SV\'s inversion.">',
'##INFO=<ID=PREDICTED_LOF,Number=.,Type=String,Description="Gene(s) on which the SV is predicted to have a loss-of-function effect.">',
'##INFO=<ID=PREDICTED_MSV_EXON_OVERLAP,Number=.,Type=String,Description="Gene(s) on which the multiallelic SV would be predicted to have a LOF, INTRAGENIC_EXON_DUP, COPY_GAIN, DUP_PARTIAL, TSS_DUP, or PARTIAL_EXON_DUP annotation if the SV were biallelic.">',
'##INFO=<ID=PREDICTED_NEAREST_TSS,Number=.,Type=String,Description="Nearest transcription start site to an intergenic variant.">',
'##INFO=<ID=PREDICTED_NONCODING_BREAKPOINT,Number=.,Type=String,Description="Class(es) of noncoding elements disrupted by SV breakpoint.">',
'##INFO=<ID=PREDICTED_NONCODING_SPAN,Number=.,Type=String,Description="Class(es) of noncoding elements spanned by SV.">',
'##INFO=<ID=PREDICTED_PARTIAL_EXON_DUP,Number=.,Type=String,Description="Gene(s) where the duplication SV has one breakpoint in the coding sequence.">',
'##INFO=<ID=PREDICTED_PROMOTER,Number=.,Type=String,Description="Gene(s) for which the SV is predicted to overlap the promoter region.">',
'##INFO=<ID=PREDICTED_TSS_DUP,Number=.,Type=String,Description="Gene(s) for which the SV is predicted to duplicate the transcription start site.">',
'##INFO=<ID=PREDICTED_UTR,Number=.,Type=String,Description="Gene(s) for which the SV is predicted to disrupt a UTR.">',
'##INFO=<ID=AN,Number=1,Type=Integer,Description="Total number of alleles genotyped (for biallelic sites) or individuals with copy-state estimates (for multiallelic sites).">',
'##INFO=<ID=AC,Number=A,Type=Integer,Description="Number of non-reference alleles observed (for biallelic sites) or individuals at each copy state (for multiallelic sites).">',
'##INFO=<ID=AF,Number=A,Type=Float,Description="Allele frequency (for biallelic sites) or copy-state frequency (for multiallelic sites).">',
'##INFO=<ID=N_BI_GENOS,Number=1,Type=Integer,Description="Total number of individuals with complete genotypes (biallelic sites only).">',
'##INFO=<ID=N_HOMREF,Number=1,Type=Integer,Description="Number of individuals with homozygous reference genotypes (biallelic sites only).">',
'##INFO=<ID=N_HET,Number=1,Type=Integer,Description="Number of individuals with heterozygous genotypes (biallelic sites only).">',
'##INFO=<ID=N_HOMALT,Number=1,Type=Integer,Description="Number of individuals with homozygous alternate genotypes (biallelic sites only).">',
'##INFO=<ID=FREQ_HOMREF,Number=1,Type=Float,Description="Homozygous reference genotype frequency (biallelic sites only).">',
'##INFO=<ID=FREQ_HET,Number=1,Type=Float,Description="Heterozygous genotype frequency (biallelic sites only).">',
'##INFO=<ID=FREQ_HOMALT,Number=1,Type=Float,Description="Homozygous alternate genotype frequency (biallelic sites only).">',
'##INFO=<ID=MALE_AN,Number=1,Type=Integer,Description="Total number of MALE alleles genotyped (for biallelic sites) or MALE individuals with copy-state estimates (for multiallelic sites).">',
'##INFO=<ID=MALE_AC,Number=A,Type=Integer,Description="Number of non-reference MALE alleles observed (for biallelic sites) or MALE individuals at each copy state (for multiallelic sites).">',
'##INFO=<ID=MALE_AF,Number=A,Type=Float,Description="MALE allele frequency (for biallelic sites) or MALE copy-state frequency (for multiallelic sites).">',
'##INFO=<ID=MALE_N_BI_GENOS,Number=1,Type=Integer,Description="Total number of MALE individuals with complete genotypes (biallelic sites only).">',
'##INFO=<ID=MALE_N_HOMREF,Number=1,Type=Integer,Description="Number of MALE individuals with homozygous reference genotypes (biallelic sites only).">',
'##INFO=<ID=MALE_N_HET,Number=1,Type=Integer,Description="Number of MALE individuals with heterozygous genotypes (biallelic sites only).">',
'##INFO=<ID=MALE_N_HOMALT,Number=1,Type=Integer,Description="Number of MALE individuals with homozygous alternate genotypes (biallelic sites only).">',
'##INFO=<ID=MALE_FREQ_HOMREF,Number=1,Type=Float,Description="MALE homozygous reference genotype frequency (biallelic sites only).">',
'##INFO=<ID=MALE_FREQ_HET,Number=1,Type=Float,Description="MALE heterozygous genotype frequency (biallelic sites only).">',
'##INFO=<ID=MALE_FREQ_HOMALT,Number=1,Type=Float,Description="MALE homozygous alternate genotype frequency (biallelic sites only).">',
'##INFO=<ID=FEMALE_AN,Number=1,Type=Integer,Description="Total number of FEMALE alleles genotyped (for biallelic sites) or FEMALE individuals with copy-state estimates (for multiallelic sites).">',
'##INFO=<ID=FEMALE_AC,Number=A,Type=Integer,Description="Number of non-reference FEMALE alleles observed (for biallelic sites) or FEMALE individuals at each copy state (for multiallelic sites).">',
'##INFO=<ID=FEMALE_AF,Number=A,Type=Float,Description="FEMALE allele frequency (for biallelic sites) or FEMALE copy-state frequency (for multiallelic sites).">',
'##INFO=<ID=FEMALE_N_BI_GENOS,Number=1,Type=Integer,Description="Total number of FEMALE individuals with complete genotypes (biallelic sites only).">',
'##INFO=<ID=FEMALE_N_HOMREF,Number=1,Type=Integer,Description="Number of FEMALE individuals with homozygous reference genotypes (biallelic sites only).">',
'##INFO=<ID=FEMALE_N_HET,Number=1,Type=Integer,Description="Number of FEMALE individuals with heterozygous genotypes (biallelic sites only).">',
'##INFO=<ID=FEMALE_N_HOMALT,Number=1,Type=Integer,Description="Number of FEMALE individuals with homozygous alternate genotypes (biallelic sites only).">',
'##INFO=<ID=FEMALE_FREQ_HOMREF,Number=1,Type=Float,Description="FEMALE homozygous reference genotype frequency (biallelic sites only).">',
'##INFO=<ID=FEMALE_FREQ_HET,Number=1,Type=Float,Description="FEMALE heterozygous genotype frequency (biallelic sites only).">',
'##INFO=<ID=FEMALE_FREQ_HOMALT,Number=1,Type=Float,Description="FEMALE homozygous alternate genotype frequency (biallelic sites only).">',
'##INFO=<ID=gnomAD_V2_SVID,Number=.,Type=String,Description="Allele frequency (for biallelic sites) or copy-state frequency (for multiallelic sites) of an overlapping event in gnomad.">',
'##INFO=<ID=gnomAD_V2_AF,Number=1,Type=Float,Description="Allele frequency (for biallelic sites) or copy-state frequency (for multiallelic sites) of an overlapping event in gnomad.">',
'##INFO=<ID=gnomAD_V2_AC_AF,Number=1,Type=Integer,Description="Allele frequency (for biallelic sites) or copy-state frequency (for multiallelic sites) of an overlapping event in gnomad.">',
'##INFO=<ID=gnomAD_V2_AN_AF,Number=1,Type=Integer,Description="Allele frequency (for biallelic sites) or copy-state frequency (for multiallelic sites) of an overlapping event in gnomad.">',
]

VCF_DATA_ROW = [
['chr1', '180928', 'BND_chr1_6', 'N', '<BND>', '657', 'HIGH_SR_BACKGROUND;UNRESOLVED', 'ALGORITHMS=manta;CHR2=chr5;END=180928;END2=20404;EVIDENCE=PE,SR;PREDICTED_INTERGENIC;PREDICTED_NEAREST_TSS=OR4F5,PLEKHG4B;PREDICTED_NONCODING_BREAKPOINT=DNase;STRANDS=+-;SVLEN=-1;SVTYPE=BND;UNRESOLVED_TYPE=SINGLE_ENDER_+-;AN=8;AC=1;AF=0.04775;N_BI_GENOS=2911;N_HOMREF=2633;N_HET=278;N_HOMALT=0;FREQ_HOMREF=0.9045;FREQ_HET=0.0954998;FREQ_HOMALT=0;MALE_AN=2894;MALE_AC=137;MALE_AF=0.047339;MALE_N_BI_GENOS=1447;MALE_N_HOMREF=1310;MALE_N_HET=137;MALE_N_HOMALT=0;MALE_FREQ_HOMREF=0.905321;MALE_FREQ_HET=0.0946786;MALE_FREQ_HOMALT=0;FEMALE_AN=2906;FEMALE_AC=139;FEMALE_AF=0.047832;FEMALE_N_BI_GENOS=1453;FEMALE_N_HOMREF=1314;FEMALE_N_HET=139;FEMALE_N_HOMALT=0;FEMALE_FREQ_HOMREF=0.904336;FEMALE_FREQ_HET=0.0956641;FEMALE_FREQ_HOMALT=0', 'GT:EV:GQ:PE_GQ:PE_GT:SR_GQ:SR_GT', '0/0:PE,SR:99:99:0:2:0', '0/1:SR:31:99:0:31:1', '0/0:PE,SR:99:99:0:99:0', '0/0:PE,SR:99:99:0:2:0', '0/0:PE,SR:99:99:0:2:0'],
['chr1', '257666', 'DUP_chr1_5', 'N', '<DUP>', '999', 'PASS', 'ALGORITHMS=depth;CHR2=chr1;END=263666;EVIDENCE=BAF,RD;PREDICTED_INTERGENIC;PREDICTED_NEAREST_TSS=OR4F29;SVLEN=6000;SVTYPE=DUP;AN=8;AC=1;AF=0.115596;N_BI_GENOS=2911;N_HOMREF=2348;N_HET=453;N_HOMALT=110;FREQ_HOMREF=0.806596;FREQ_HET=0.155617;FREQ_HOMALT=0.0377877;MALE_AN=2894;MALE_AC=339;MALE_AF=0.117139;MALE_N_BI_GENOS=1447;MALE_N_HOMREF=1163;MALE_N_HET=229;MALE_N_HOMALT=55;MALE_FREQ_HOMREF=0.803732;MALE_FREQ_HET=0.158258;MALE_FREQ_HOMALT=0.0380097;FEMALE_AN=2906;FEMALE_AC=330;FEMALE_AF=0.113558;FEMALE_N_BI_GENOS=1453;FEMALE_N_HOMREF=1178;FEMALE_N_HET=220;FEMALE_N_HOMALT=55;FEMALE_FREQ_HOMREF=0.810736;FEMALE_FREQ_HET=0.151411;FEMALE_FREQ_HOMALT=0.0378527', 'GT:EV:GQ:RD_CN:RD_GQ', '0/0:RD:99:2:99', '0/0:RD:99:2:99', '0/1:RD:8:3:8', '0/0:RD:13:1:13', '0/0:RD:13:1:13'],
['chr1', '413968', 'DEL_chr1_12', 'N', '<DEL>', '999', 'PASS', 'ALGORITHMS=depth;CHR2=chr1;END=428500;EVIDENCE=RD;PREDICTED_INTERGENIC;PREDICTED_NEAREST_TSS=OR4F29;SVLEN=14532;SVTYPE=DEL;AN=8;AC=1;AF=0.064926;N_BI_GENOS=2911;N_HOMREF=2538;N_HET=368;N_HOMALT=5;FREQ_HOMREF=0.871865;FREQ_HET=0.126417;FREQ_HOMALT=0.00171762;MALE_AN=2894;MALE_AC=172;MALE_AF=0.059433;MALE_N_BI_GENOS=1447;MALE_N_HOMREF=1278;MALE_N_HET=166;MALE_N_HOMALT=3;MALE_FREQ_HOMREF=0.883207;MALE_FREQ_HET=0.11472;MALE_FREQ_HOMALT=0.00207325;FEMALE_AN=2906;FEMALE_AC=205;FEMALE_AF=0.070544;FEMALE_N_BI_GENOS=1453;FEMALE_N_HOMREF=1250;FEMALE_N_HET=201;FEMALE_N_HOMALT=2;FEMALE_FREQ_HOMREF=0.860289;FEMALE_FREQ_HET=0.138334;FEMALE_FREQ_HOMALT=0.00137646', 'GT:EV:GQ:RD_CN:RD_GQ', '0/0:RD:12:2:12', '0/0:RD:0:2:0', '0/1:RD:99:1:99', '0/0:RD:2:2:2', '0/0:RD:2:2:2'],
['chr1', '789481', 'BND_chr1_9', 'N', '<BND>', '999', 'PESR_GT_OVERDISPERSION;UNRESOLVED', 'ALGORITHMS=manta;CHR2=chr1;END=789481;EVIDENCE=PE;PREDICTED_INTERGENIC;PREDICTED_NEAREST_TSS=FBXO28,OR4F16;PREDICTED_NONCODING_BREAKPOINT=DNase,Tommerup_TADanno;STRANDS=-+;SVLEN=223225007;SVTYPE=BND;UNRESOLVED_TYPE=SINGLE_ENDER_-+;AN=8;AC=7;AF=0.910684;N_BI_GENOS=2911;N_HOMREF=0;N_HET=520;N_HOMALT=2391;FREQ_HOMREF=0;FREQ_HET=0.178633;FREQ_HOMALT=0.821367;MALE_AN=2894;MALE_AC=2639;MALE_AF=0.911887;MALE_N_BI_GENOS=1447;MALE_N_HOMREF=0;MALE_N_HET=255;MALE_N_HOMALT=1192;MALE_FREQ_HOMREF=0;MALE_FREQ_HET=0.176227;MALE_FREQ_HOMALT=0.823773;FEMALE_AN=2906;FEMALE_AC=2643;FEMALE_AF=0.909498;FEMALE_N_BI_GENOS=1453;FEMALE_N_HOMREF=0;FEMALE_N_HET=263;FEMALE_N_HOMALT=1190;FEMALE_FREQ_HOMREF=0;FEMALE_FREQ_HET=0.181005;FEMALE_FREQ_HOMALT=0.818995', 'GT:EV:GQ:PE_GQ:PE_GT:SR_GQ:SR_GT', '1/1:PE:59:59:2:99:0', '1/1:PE:26:26:2:99:0', '1/1:PE:39:39:2:99:0', '0/1:PE:19:19:1:99:0', '0/1:PE:19:19:1:99:0'],
['chr1', '4228405', 'INS_chr1_65', 'N', '<INS:ME:ALU>', '605', 'HIGH_SR_BACKGROUND', 'ALGORITHMS=manta,melt;CHR2=chr1;END=4228448;EVIDENCE=SR;gnomAD_V2_SVID=gnomAD-SV_v2.1_INS_chr1_65;gnomAD_V2_AF=0.068962998688221;gnomAD_V2_AC_AF=224;gnomAD_V2_AN_AF=3247;PREDICTED_INTERGENIC;PREDICTED_NEAREST_TSS=C1orf174;SVLEN=298;SVTYPE=INS;AN=8;AC=1;AF=0.10237;N_BI_GENOS=2911;N_HOMREF=2318;N_HET=590;N_HOMALT=3;FREQ_HOMREF=0.79629;FREQ_HET=0.202679;FREQ_HOMALT=0.00103057;MALE_AN=2894;MALE_AC=293;MALE_AF=0.101244;MALE_N_BI_GENOS=1447;MALE_N_HOMREF=1156;MALE_N_HET=289;MALE_N_HOMALT=2;MALE_FREQ_HOMREF=0.798894;MALE_FREQ_HET=0.199724;MALE_FREQ_HOMALT=0.00138217;FEMALE_AN=2906;FEMALE_AC=302;FEMALE_AF=0.103923;FEMALE_N_BI_GENOS=1453;FEMALE_N_HOMREF=1152;FEMALE_N_HET=300;FEMALE_N_HOMALT=1;FEMALE_FREQ_HOMREF=0.792842;FEMALE_FREQ_HET=0.206469;FEMALE_FREQ_HOMALT=0.000688231', 'GT:EV:GQ:PE_GQ:PE_GT:SR_GQ:SR_GT', '0/1:SR:62:99:0:62:1', '0/0:PE,SR:99:99:0:99:0', '0/0:PE,SR:99:99:0:99:0', '0/0:PE,SR:99:99:0:99:0', '0/0:PE,SR:99:99:0:99:0'],
['chr1', '6558902', 'CPX_chr1_22', 'N', '<CPX>', '644', 'BOTHSIDES_SUPPORT;HIGH_SR_BACKGROUND', 'ALGORITHMS=manta;CHR2=chr1;CPX_INTERVALS=INV_chr1:6558902-6559723,DUP_chr1:6559655-6559723;CPX_TYPE=INVdup;END=6559723;EVIDENCE=PE,SR;PREDICTED_INTRONIC=TAS1R1;PREDICTED_NONCODING_BREAKPOINT=Tommerup_TADanno;PREDICTED_NONCODING_SPAN=DNase;SVLEN=821;SVTYPE=CPX;AN=8;AC=2;AF=0.169873;N_BI_GENOS=2911;N_HOMREF=1925;N_HET=983;N_HOMALT=3;FREQ_HOMREF=0.661285;FREQ_HET=0.337685;FREQ_HOMALT=0.00103057;MALE_AN=2894;MALE_AC=497;MALE_AF=0.171735;MALE_N_BI_GENOS=1447;MALE_N_HOMREF=950;MALE_N_HET=497;MALE_N_HOMALT=0;MALE_FREQ_HOMREF=0.656531;MALE_FREQ_HET=0.343469;MALE_FREQ_HOMALT=0;FEMALE_AN=2906;FEMALE_AC=488;FEMALE_AF=0.167928;FEMALE_N_BI_GENOS=1453;FEMALE_N_HOMREF=968;FEMALE_N_HET=482;FEMALE_N_HOMALT=3;FEMALE_FREQ_HOMREF=0.666208;FEMALE_FREQ_HET=0.331727;FEMALE_FREQ_HOMALT=0.00206469', 'GT:EV:GQ:PE_GQ:PE_GT:SR_GQ:SR_GT:RD_CN', '0/0:PE,SR:99:99:0:99:0:2', '0/1:PE,SR:57:0:0:57:1:2', '0/1:PE,SR:0:0:1:99:0:2', '0/0:PE,SR:99:99:0:0:0:3', '0/0:PE,SR:99:99:0:0:0:1'],
['chr1', '16088760', 'CPX_chr1_41', 'N', '<CPX>', '684', 'PASS', 'ALGORITHMS=manta;CHR2=chr1;CPX_INTERVALS=DUP_chr1:16088760-16088835,INV_chr1:16088760-16089601;CPX_TYPE=dupINV;END=16089601;EVIDENCE=PE,SR;PREDICTED_INTERGENIC;PREDICTED_NEAREST_TSS=FAM131C;PREDICTED_NONCODING_BREAKPOINT=Tommerup_TADanno;PREDICTED_NONCODING_SPAN=DNase;SVLEN=841;SVTYPE=CPX;AN=8;AC=2;AF=0.218138;N_BI_GENOS=2911;N_HOMREF=1659;N_HET=1234;N_HOMALT=18;FREQ_HOMREF=0.569907;FREQ_HET=0.423909;FREQ_HOMALT=0.00618344;MALE_AN=2894;MALE_AC=635;MALE_AF=0.219419;MALE_N_BI_GENOS=1447;MALE_N_HOMREF=818;MALE_N_HET=623;MALE_N_HOMALT=6;MALE_FREQ_HOMREF=0.565308;MALE_FREQ_HET=0.430546;MALE_FREQ_HOMALT=0.00414651;FEMALE_AN=2906;FEMALE_AC=629;FEMALE_AF=0.216449;FEMALE_N_BI_GENOS=1453;FEMALE_N_HOMREF=836;FEMALE_N_HET=605;FEMALE_N_HOMALT=12;FEMALE_FREQ_HOMREF=0.575361;FEMALE_FREQ_HET=0.41638;FEMALE_FREQ_HOMALT=0.00825877', 'GT:EV:GQ:PE_GQ:PE_GT:SR_GQ:SR_GT', '0/1:SR:52:0:0:52:1', '0/0:PE,SR:99:99:0:99:0', '0/0:PE,SR:99:99:0:99:0', '0/1:SR:62:0:0:62:1', '0/1:SR:62:0:0:62:1'],
['chr1', '17465707', 'INS_chr1_268', 'N', '<INS:ME:SVA>', '263', 'HIGH_SR_BACKGROUND', 'ALGORITHMS=melt;CHR2=chr1;END=17465723;EVIDENCE=SR;PREDICTED_INTERGENIC;PREDICTED_NEAREST_TSS=RCC2;PREDICTED_NONCODING_BREAKPOINT=Tommerup_TADanno;SVLEN=955;SVTYPE=INS;AN=8;AC=1;AF=0.004466;N_BI_GENOS=2911;N_HOMREF=2885;N_HET=26;N_HOMALT=0;FREQ_HOMREF=0.991068;FREQ_HET=0.00893164;FREQ_HOMALT=0;MALE_AN=2894;MALE_AC=14;MALE_AF=0.004838;MALE_N_BI_GENOS=1447;MALE_N_HOMREF=1433;MALE_N_HET=14;MALE_N_HOMALT=0;MALE_FREQ_HOMREF=0.990325;MALE_FREQ_HET=0.00967519;MALE_FREQ_HOMALT=0;FEMALE_AN=2906;FEMALE_AC=11;FEMALE_AF=0.003785;FEMALE_N_BI_GENOS=1453;FEMALE_N_HOMREF=1442;FEMALE_N_HET=11;FEMALE_N_HOMALT=0;FEMALE_FREQ_HOMREF=0.992429;FEMALE_FREQ_HET=0.00757054;FEMALE_FREQ_HOMALT=0', 'GT:EV:GQ:PE_GQ:PE_GT:SR_GQ:SR_GT', '0/0:PE,SR:99:99:0:99:0', '0/0:PE,SR:99:99:0:99:0', '0/1:SR:0:99:0:0:1', '0/0:PE,SR:99:99:0:2:0', '0/0:PE,SR:99:99:0:2:0'],
['chr1', '21427498', 'CPX_chr1_54', 'N', '<CPX>', '733', 'PASS', 'ALGORITHMS=manta;CHR2=chr1;CPX_INTERVALS=DUP_chr1:21427498-21427959,INV_chr1:21427498-21480073,DEL_chr1:21480073-21480419;CPX_TYPE=dupINVdel;END=21480419;EVIDENCE=PE;PREDICTED_LOF=NBPF3;PREDICTED_NONCODING_BREAKPOINT=DNase,Tommerup_TADanno;PREDICTED_NONCODING_SPAN=DNase;SVLEN=52921;SVTYPE=CPX;AN=8;AC=4;AF=0.499656;N_BI_GENOS=2911;N_HOMREF=51;N_HET=2811;N_HOMALT=49;FREQ_HOMREF=0.0175198;FREQ_HET=0.965648;FREQ_HOMALT=0.0168327;MALE_AN=2894;MALE_AC=1453;MALE_AF=0.502073;MALE_N_BI_GENOS=1447;MALE_N_HOMREF=19;MALE_N_HET=1403;MALE_N_HOMALT=25;MALE_FREQ_HOMREF=0.0131306;MALE_FREQ_HET=0.969592;MALE_FREQ_HOMALT=0.0172771;FEMALE_AN=2906;FEMALE_AC=1445;FEMALE_AF=0.497247;FEMALE_N_BI_GENOS=1453;FEMALE_N_HOMREF=32;FEMALE_N_HET=1397;FEMALE_N_HOMALT=24;FEMALE_FREQ_HOMREF=0.0220234;FEMALE_FREQ_HET=0.961459;FEMALE_FREQ_HOMALT=0.0165175', 'GT:EV:GQ:PE_GQ:PE_GT:SR_GQ:SR_GT', '0/1:PE:93:93:1:99:0', '0/1:PE:79:79:1:99:0', '0/1:PE:33:33:1:6:0', '0/1:PE,SR:39:39:1:99:0', '0/1:PE,SR:39:39:1:99:0'],
['chr1', '48963084', 'INS_chr1_688', 'N', '<INS:ME:LINE1>', '526', 'HIGH_SR_BACKGROUND', 'ALGORITHMS=melt;CHR2=chr1;END=48963135;EVIDENCE=SR;PREDICTED_INTRONIC=AGBL4;PREDICTED_NONCODING_BREAKPOINT=Tommerup_TADanno;SVLEN=5520;SVTYPE=INS;AN=8;AC=1;AF=0.06338;N_BI_GENOS=2911;N_HOMREF=2544;N_HET=365;N_HOMALT=2;FREQ_HOMREF=0.873926;FREQ_HET=0.125386;FREQ_HOMALT=0.000687049;MALE_AN=2894;MALE_AC=177;MALE_AF=0.061161;MALE_N_BI_GENOS=1447;MALE_N_HOMREF=1271;MALE_N_HET=175;MALE_N_HOMALT=1;MALE_FREQ_HOMREF=0.878369;MALE_FREQ_HET=0.12094;MALE_FREQ_HOMALT=0.000691085;FEMALE_AN=2906;FEMALE_AC=192;FEMALE_AF=0.06607;FEMALE_N_BI_GENOS=1453;FEMALE_N_HOMREF=1262;FEMALE_N_HET=190;FEMALE_N_HOMALT=1;FEMALE_FREQ_HOMREF=0.868548;FEMALE_FREQ_HET=0.130764;FEMALE_FREQ_HOMALT=0.000688231', 'GT:EV:GQ:PE_GQ:PE_GT:SR_GQ:SR_GT', '0/0:PE,SR:99:99:0:99:0', '0/0:PE,SR:99:99:0:99:0', '0/0:PE,SR:99:99:0:99:0', '0/1:SR:0:99:0:0:1', '0/1:SR:0:99:0:0:1'],
['chr1', '180540234', 'CPX_chr1_251', 'N', '<CPX>', '999', 'UNRESOLVED', 'ALGORITHMS=manta;CHR2=chr1;CPX_INTERVALS=DEL_chr1:180540234-181074767,INV_chr1:181074767-181074938;CPX_TYPE=delINV;END=181074952;EVIDENCE=PE,SR;PREDICTED_LOF=KIAA1614,MR1,STX6,XPR1;PREDICTED_NONCODING_BREAKPOINT=Tommerup_TADanno;PREDICTED_NONCODING_SPAN=DNase,Enhancer;SVLEN=534718;SVTYPE=CPX;UNRESOLVED_TYPE=POSTHOC_RD_GT_REJECTION;AN=8;AC=3;AF=0.251804;N_BI_GENOS=2911;N_HOMREF=1559;N_HET=1238;N_HOMALT=114;FREQ_HOMREF=0.535555;FREQ_HET=0.425283;FREQ_HOMALT=0.0391618;MALE_AN=2894;MALE_AC=724;MALE_AF=0.250173;MALE_N_BI_GENOS=1447;MALE_N_HOMREF=784;MALE_N_HET=602;MALE_N_HOMALT=61;MALE_FREQ_HOMREF=0.541811;MALE_FREQ_HET=0.416033;MALE_FREQ_HOMALT=0.0421562;FEMALE_AN=2906;FEMALE_AC=736;FEMALE_AF=0.253269;FEMALE_N_BI_GENOS=1453;FEMALE_N_HOMREF=770;FEMALE_N_HET=630;FEMALE_N_HOMALT=53;FEMALE_FREQ_HOMREF=0.529938;FEMALE_FREQ_HET=0.433586;FEMALE_FREQ_HOMALT=0.0364763', 'GT:EV:GQ:PE_GQ:PE_GT:SR_GQ:SR_GT', '0/0:PE,SR:99:99:0:99:0', '0/1:PE,SR:41:26:1:41:1', '1/1:PE,SR:89:33:1:89:2', '0/0:PE,SR:99:99:0:99:0', '0/0:PE,SR:99:99:0:99:0'],
]

VCF_DATA = VCF_HEADER_META + [
'#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE-1	SAMPLE-2	SAMPLE-3	SAMPLE-4	SAMPLE-5'
] + ['\t'.join(row) for row in VCF_DATA_ROW]

STRVCTVRE_DATA = ['0.7', '0.8', '0.9', '0.71', '0.73', '0.74', '0.75', '0.76', '0.77', '0.78', '0.79']
STRVCTVRE_VCF_DATA_ROW = copy.deepcopy(VCF_DATA_ROW)
for i, row in enumerate(STRVCTVRE_VCF_DATA_ROW):
    row[7] = f'StrVCTVRE={STRVCTVRE_DATA[i]};{row[7]}'
STRVCTVRE_VCF_DATA = VCF_HEADER_META + [
'##INFO=<ID=StrVCTVRE,Number=1,Type=String,Description="StrVCTVRE score.">',
'#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE-1	SAMPLE-2	SAMPLE-3	SAMPLE-4	SAMPLE-5',
] + ['\t'.join(row) for row in STRVCTVRE_VCF_DATA_ROW]

SAMPLES_GQ_SV_FIELDS = ['samples_gq_sv_{}_to_{}'.format(i, i+10) for i in range(0, 1000, 10)]

DATA_FIELDS = [
    'contig', 'sc', 'sf', 'sn', 'start', 'end', 'sv_callset_Het', 'sv_callset_Hom', 'gnomad_svs_ID', 'gnomad_svs_AF',
    'gnomad_svs_AC', 'gnomad_svs_AN', 'pos', 'filters', 'bothsides_support', 'algorithms', 'xpos', 'cpx_intervals',
    'xstart', 'xstop', 'rg37_locus', 'rg37_locus_end', 'svType', 'transcriptConsequenceTerms', 'sv_type_detail',
    'geneIds', 'samples_no_call', 'samples_num_alt_1', 'samples_num_alt_2', 'variantId', 'sortedTranscriptConsequences',
    'genotypes']

DATA_FIELDS += SAMPLES_GQ_SV_FIELDS
DATA_FIELDS.append('StrVCTVRE_score')

EXPECTED_SAMPLE_GQ = [
    {
        'samples_gq_sv_10_to_20': ['SAMPLE-4', 'SAMPLE-5'],
        'samples_gq_sv_20_to_30': ['SAMPLE-2'],
        'samples_gq_sv_30_to_40': ['SAMPLE-3'],
        'samples_gq_sv_50_to_60': ['SAMPLE-1'],
    },
    {
        'samples_gq_sv_60_to_70': ['SAMPLE-1'],
        'samples_gq_sv_90_to_100': ['SAMPLE-2', 'SAMPLE-3', 'SAMPLE-4', 'SAMPLE-5'],
    },
    {
        'samples_gq_sv_0_to_10': ['SAMPLE-3'],
        'samples_gq_sv_50_to_60': ['SAMPLE-2'],
        'samples_gq_sv_90_to_100': ['SAMPLE-1', 'SAMPLE-4', 'SAMPLE-5'],
    }
]

EXPECTED_DATA = [
    hl.Struct(
        contig='1', sc=7, sf=0.910684, sn=8, start=789481, end=789481, sv_callset_Het=520, sv_callset_Hom=2391,
        gnomad_svs_ID=None, gnomad_svs_AF=None, gnomad_svs_AC=None, gnomad_svs_AN=None, pos=789481,
        filters=['PESR_GT_OVERDISPERSION', 'UNRESOLVED'], bothsides_support=False, algorithms=['manta'],
        xpos=1000789481, cpx_intervals=None, xstart=1000789481, xstop=1000789481,
        rg37_locus=hl.Locus(contig=1, position=724861, reference_genome='GRCh37'),
        rg37_locus_end=hl.Locus(contig=1, position=724861, reference_genome='GRCh37'), svType='BND',
        transcriptConsequenceTerms=['NEAREST_TSS', 'NEAREST_TSS', 'BND'], sv_type_detail=None,
        geneIds=frozenset(), samples_no_call=[], samples_num_alt_1=['SAMPLE-4', 'SAMPLE-5'],
        samples_num_alt_2=['SAMPLE-1', 'SAMPLE-2', 'SAMPLE-3'], variantId='BND_chr1_9',
        sortedTranscriptConsequences=[
            hl.Struct(gene_symbol='FBXO28', gene_id='ENSG00000143756', major_consequence='NEAREST_TSS'),
            hl.Struct(gene_symbol='OR4F16', gene_id='ENSG00000186192', major_consequence='NEAREST_TSS')],
        genotypes=[hl.Struct(sample_id='SAMPLE-1', gq=59, cn=None, num_alt=2),
                   hl.Struct(sample_id='SAMPLE-2', gq=26, cn=None, num_alt=2),
                   hl.Struct(sample_id='SAMPLE-3', gq=39, cn=None, num_alt=2),
                   hl.Struct(sample_id='SAMPLE-4', gq=19, cn=None, num_alt=1),
                   hl.Struct(sample_id='SAMPLE-5', gq=19, cn=None, num_alt=1)],
        StrVCTVRE_score=0.71,
        **{key: EXPECTED_SAMPLE_GQ[0].get(key) for key in SAMPLES_GQ_SV_FIELDS}
    ),
    hl.Struct(
        contig='1', sc=1, sf=0.10237, sn=8, start=4228405, end=4228448, sv_callset_Het=590, sv_callset_Hom=3,
        gnomad_svs_ID='gnomAD-SV_v2.1_INS_chr1_65', gnomad_svs_AF=0.068962998688221, gnomad_svs_AC=224,
        gnomad_svs_AN=3247, pos=4228405,
        filters=['HIGH_SR_BACKGROUND'], bothsides_support=False, algorithms=['manta', 'melt'], xpos=1004228405,
        cpx_intervals=None, xstart=1004228405, xstop=1004228448,
        rg37_locus=hl.Locus(contig=1, position=4288465, reference_genome='GRCh37'),
        rg37_locus_end=hl.Locus(contig=1, position=4288508, reference_genome='GRCh37'), svType='INS',
        transcriptConsequenceTerms=['NEAREST_TSS', 'INS'], sv_type_detail='ME:ALU', geneIds=frozenset(),
        samples_no_call=[], samples_num_alt_1=['SAMPLE-1'], samples_num_alt_2=[],
        variantId='INS_chr1_65', sortedTranscriptConsequences=[
            hl.Struct(gene_symbol='C1orf174', gene_id='ENSG00000198912', major_consequence='NEAREST_TSS')],
        genotypes=[hl.Struct(sample_id='SAMPLE-1', gq=62, cn=None, num_alt=1),
                   hl.Struct(sample_id='SAMPLE-2', gq=99, cn=None, num_alt=0),
                   hl.Struct(sample_id='SAMPLE-3', gq=99, cn=None, num_alt=0),
                   hl.Struct(sample_id='SAMPLE-4', gq=99, cn=None, num_alt=0),
                   hl.Struct(sample_id='SAMPLE-5', gq=99, cn=None, num_alt=0)],
        StrVCTVRE_score=0.73,
        **{key: EXPECTED_SAMPLE_GQ[1].get(key) for key in SAMPLES_GQ_SV_FIELDS}
    ),
    hl.Struct(
        contig='1', sc=2, sf=0.169873, sn=8, start=6558902, end=6559723, sv_callset_Het=983, sv_callset_Hom=3,
        gnomad_svs_ID=None, gnomad_svs_AF=None, gnomad_svs_AC=None, gnomad_svs_AN=None, pos=6558902,
        filters=['HIGH_SR_BACKGROUND'], bothsides_support=True, algorithms=['manta'], xpos=1006558902,
        cpx_intervals=[hl.Struct(type='INV', chrom='1', start=6558902, end=6559723),
                       hl.Struct(type='DUP', chrom='1', start=6559655, end=6559723)], xstart=1006558902,
        xstop=1006559723, rg37_locus=hl.Locus(contig=1, position=6618962, reference_genome='GRCh37'),
        rg37_locus_end=hl.Locus(contig=1, position=6619783, reference_genome='GRCh37'), svType='CPX',
        transcriptConsequenceTerms=['INTRONIC', 'CPX'], sv_type_detail='INVdup',
        geneIds=frozenset({'ENSG00000173662'}), samples_no_call=[], samples_num_alt_1=['SAMPLE-2', 'SAMPLE-3'],
        samples_num_alt_2=[], variantId='CPX_chr1_22', sortedTranscriptConsequences=[
            hl.Struct(gene_symbol='TAS1R1', gene_id='ENSG00000173662', major_consequence='INTRONIC')],
        genotypes=[hl.Struct(sample_id='SAMPLE-1', gq=99, cn=2, num_alt=0),
                   hl.Struct(sample_id='SAMPLE-2', gq=57, cn=2, num_alt=1),
                   hl.Struct(sample_id='SAMPLE-3', gq=0, cn=2, num_alt=1),
                   hl.Struct(sample_id='SAMPLE-4', gq=99, cn=3, num_alt=0),
                   hl.Struct(sample_id='SAMPLE-5', gq=99, cn=1, num_alt=0)],
        StrVCTVRE_score=0.74,
        **{key: EXPECTED_SAMPLE_GQ[2].get(key) for key in SAMPLES_GQ_SV_FIELDS}
    ),
]

TEST_GUID = 'test_guid'
TEST_PASSWORD = 'ExamplePasswd'
TEST_INPUT_DATASET = 'test_dataset/input_vcf.gz'
TEST_INPUT_VCF_NO_EXT = 'test_dataset/input_vcf'
TEST_INPUT_MT_PATH = 'test_dataset/input_vcf.mt'
TEST_GENCODE_PATH = 'test_gtf/gtf'
TEST_HOST = 'TEST_HOST'
TEST_PORT = '9500'
TEST_GENCODE_RELEASE = 31
TEST_BLOCK_SIZE = 3000
TEST_NUM_SHARDS = 10
TEST_INDEX_NAME = 'test_guid__structural_variants__wgs__grch38__20210426'


class LoadDataTest(unittest.TestCase):
    def setUp(self):
        self.vcf_file = tempfile.mkstemp(suffix='.vcf')[1]
        with open(self.vcf_file, 'w') as f:
            f.writelines('\n'.join(VCF_DATA))
        self.strvctvre_vcf_file = tempfile.mkstemp(suffix='.vcf')[1]
        with open(self.strvctvre_vcf_file, 'w') as f:
            f.writelines('\n'.join(STRVCTVRE_VCF_DATA))
        self.mt = hl.import_vcf(self.vcf_file, reference_genome='GRCh38', force=True)
        self.strvctvre_mt = hl.import_vcf(self.strvctvre_vcf_file, reference_genome='GRCh38', force=True)

    def tearDown(self):
        os.remove(self.vcf_file)
        os.remove(self.strvctvre_vcf_file)

    @mock.patch('sv_pipeline.genome.load_data.path_exists')
    @mock.patch('sv_pipeline.genome.load_data.os')
    @mock.patch('sv_pipeline.genome.load_data.hl')
    @mock.patch('sv_pipeline.genome.load_data.logger')
    def test_load_mt(self, mock_logger, mock_hl, mock_os, mock_path_exists):
        # test loading from a saved MatrixTable file
        mock_os.path.splitext.return_value = [TEST_INPUT_VCF_NO_EXT, 'gz']
        mock_path_exists.return_value = True
        _ = load_mt(TEST_INPUT_DATASET, None, False)
        mock_hl.import_vcf.assert_not_called()
        mock_os.path.splitext.assert_called_with(TEST_INPUT_DATASET)
        mock_path_exists.assert_called_with(TEST_INPUT_MT_PATH)
        mock_hl.read_matrix_table.assert_called_with(TEST_INPUT_MT_PATH)
        mock_logger.info.assert_called_with('Use the existing MatrixTable file test_dataset/input_vcf.mt. '
                                       'If the input VCF file has been changed, or you just want to re-import VCF,'
                                       ' please add "--overwrite-matrixtable" command line option.')

        # test overwriting existing MatrixTable file even if it exists
        mock_logger.reset_mock()
        mock_hl.reset_mock()
        _ = load_mt(TEST_INPUT_DATASET, TEST_INPUT_MT_PATH, True)
        mock_hl.import_vcf.assert_called_with(TEST_INPUT_DATASET, reference_genome='GRCh38')
        mock_hl.import_vcf.return_value.write.assert_called_with(TEST_INPUT_MT_PATH, overwrite=True)
        mock_hl.read_matrix_table.assert_called_with(TEST_INPUT_MT_PATH)
        mock_logger.info.assert_called_with(
            f'The VCF file has been imported to the MatrixTable at {TEST_INPUT_MT_PATH}.')

        # test the MatrixTable doesn't exist
        mock_logger.reset_mock()
        mock_hl.reset_mock()
        mock_path_exists.return_value = False
        _ = load_mt(TEST_INPUT_DATASET, None, False)
        mock_hl.import_vcf.assert_called_with(TEST_INPUT_DATASET, reference_genome='GRCh38')
        mock_hl.import_vcf.return_value.write.assert_called_with(TEST_INPUT_MT_PATH, overwrite=True)
        mock_hl.read_matrix_table.assert_called_with(TEST_INPUT_MT_PATH)
        mock_logger.info.assert_called_with(
            f'The VCF file has been imported to the MatrixTable at {TEST_INPUT_MT_PATH}.')

    @mock.patch('sv_pipeline.genome.load_data.get_sample_subset')
    @mock.patch('sv_pipeline.genome.load_data.get_sample_remap')
    @mock.patch('sv_pipeline.genome.load_data.logger')
    def test_subset_mt(self, mock_logger, mock_get_remap, mock_get_sample):
        # test missing sample exception
        mock_get_sample.return_value = {'SAMPLE-1', 'SAMPLE-3', 'SAMPLE-5', 'SAMPLE-6'}
        with self.assertRaises(Exception) as e:
            _ = subset_mt('test_guid', self.mt, skip_sample_subset=False, ignore_missing_samples=False)
        self.assertEqual(str(e.exception), 'Missing the following 1 samples:\nSAMPLE-6')

        # test remapping sample ID
        mock_get_remap.return_value = {'SAMPLE-1': 'SAMPLE-1-REMAP'}
        mt = subset_mt('test_guid', self.mt, skip_sample_subset=False, ignore_missing_samples=True)
        mock_logger.info.assert_has_calls([
            mock.call('Missing the following 1 samples:\nSAMPLE-6'),
            mock.call('Subsetting to 4 samples (remapping 1 samples)'),
        ])
        mock_get_sample.assert_called_with('test_guid', 'WGS', filename=None)
        mock_get_remap.assert_called_with('test_guid', 'WGS')
        self.assertEqual(mt.count(), (10, 3))
        self.assertEqual(mt.aggregate_cols(hl.agg.collect_as_set(mt.s)), {'SAMPLE-1-REMAP', 'SAMPLE-3', 'SAMPLE-5'})

        # test skipping sample subsetting
        mock_logger.reset_mock()
        mt = subset_mt('test_guid', self.mt, skip_sample_subset=True)
        self.assertEqual(mt.count(), (11, 5))
        self.assertEqual(mt.aggregate_cols(hl.agg.collect_as_set(mt.s)), {'SAMPLE-1', 'SAMPLE-2', 'SAMPLE-3', 'SAMPLE-4', 'SAMPLE-5'})

        # test no subsetting sample found
        mock_logger.reset_mock()
        mock_get_sample.return_value = {'sample'}
        mock_get_remap.return_value = {}
        mt = subset_mt('test_guid', self.mt, skip_sample_subset=False, ignore_missing_samples=True)
        self.assertEqual(mt.count(), (0, 0))
        calls = [
            mock.call('Missing the following 1 samples:\nsample'),
            mock.call('Subsetting to 1 samples'),
        ]
        mock_logger.info.assert_has_calls(calls)

    @mock.patch('sv_pipeline.genome.load_data.os')
    @mock.patch('sv_pipeline.genome.load_data.HailElasticsearchClient')
    @mock.patch('sv_pipeline.genome.load_data.get_es_index_name')
    def test_export_to_es(self, mock_get_index, mock_es_client, mock_os):
        mock_get_index.return_value = TEST_INDEX_NAME
        mock_os.environ.get.return_value = TEST_PASSWORD
        mock_es = mock_es_client.return_value
        rows = self.mt.rows().head(5)
        export_to_es(rows, TEST_INPUT_DATASET, TEST_GUID, TEST_HOST, TEST_PORT, TEST_PASSWORD, TEST_BLOCK_SIZE, TEST_NUM_SHARDS, es_nodes_wan_only='false')
        mock_get_index.assert_called_with(TEST_GUID, {'genomeVersion': '38', 'sampleType': WGS_SAMPLE_TYPE,
                                                      'datasetType': 'SV', 'sourceFilePath': TEST_INPUT_DATASET})
        mock_es_client.assert_called_with(host=TEST_HOST, port=TEST_PORT, es_password=TEST_PASSWORD)
        mock_es.export_table_to_elasticsearch.assert_called_with(
            mock.ANY,
            index_name=TEST_INDEX_NAME,
            block_size=TEST_BLOCK_SIZE,
            num_shards=TEST_NUM_SHARDS,
            delete_index_before_exporting=True,
            export_globals_to_index_meta=True,
            verbose=True,
            elasticsearch_mapping_id='variantId',
            elasticsearch_config={'es.nodes.wan.only': 'false'},
            func_to_run_after_index_exists=mock.ANY,
        )
        self.assertEqual(hl.eval(mock_es.export_table_to_elasticsearch.call_args.args[0].globals),
                         hl.eval(hl.struct(genomeVersion='38', sampleType=WGS_SAMPLE_TYPE,
                                           datasetType='SV', sourceFilePath=TEST_INPUT_DATASET)))

    @mock.patch('sv_pipeline.genome.load_data.load_mt')
    @mock.patch('sv_pipeline.genome.load_data.export_to_es')
    @mock.patch('sv_pipeline.genome.load_data.subset_mt')
    @mock.patch('sv_pipeline.genome.load_data.load_gencode')
    def test_main(self, mock_load_gencode, mock_subset, mock_export, mock_load_mt):
        mock_load_gencode.return_value = GENE_ID_MAPPING

        # test a normal case
        sys.argv[1:] = ['data.vcf.bgz', '--skip-sample-subset', '--ignore-missing-samples', '--project-guid', TEST_GUID,
                        '--matrixtable-file', TEST_INPUT_MT_PATH,
                        '--strvctvre', 'annotated.STRVCTVRE.fixed.vcf.gz', '--es-password', TEST_PASSWORD,
                        '--gencode-release', str(TEST_GENCODE_RELEASE), '--gencode-path', TEST_GENCODE_PATH,
                        '--es-host', TEST_HOST, '--es-port', str(TEST_PORT), '--block-size', str(TEST_BLOCK_SIZE),
                        '--num-shards', str(TEST_NUM_SHARDS), '--es-nodes-wan-only'
                        ]
        mock_load_mt.side_effect = [self.mt, self.strvctvre_mt]
        mock_subset.return_value = self.mt
        main()
        mock_load_mt.assert_has_calls([
            mock.call('data.vcf.bgz', TEST_INPUT_MT_PATH, False),
            mock.call('annotated.STRVCTVRE.fixed.vcf.gz', None, False)
        ])
        mock_load_gencode.assert_called_with(TEST_GENCODE_RELEASE, download_path=TEST_GENCODE_PATH)
        mock_subset.assert_called_with(TEST_GUID, self.mt, skip_sample_subset=True, ignore_missing_samples=True,
                                       id_file=None)
        mock_export.assert_called_with(mock.ANY, 'data.vcf.bgz', TEST_GUID, TEST_HOST, TEST_PORT, TEST_PASSWORD,
                                       TEST_BLOCK_SIZE, TEST_NUM_SHARDS, 'true')
        annoted_data = mock_export.call_args.args[0]
        self.assertListEqual([key for key in annoted_data.__dict__.keys() if not key.startswith('_')], DATA_FIELDS)
        self.assertEqual(annoted_data.count(), 11)
        data = annoted_data.order_by(annoted_data.start).tail(8).take(3)
        self.assertListEqual(data, EXPECTED_DATA)
