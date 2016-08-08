
```
dmz-seqr-db1:~ 1001 0 $ time hail importvcf  ../cseed/INMR_v9.vep.vcf.bgz splitmulti write -o INMR_v9.vds
hail: info: running: importvcf ../cseed/INMR_v9.vep.vcf.bgz
hail: info: running: splitmulti
hail: info: running: write -o INMR_v9.vds
[Stage 0:=================================================>         (5 + 1) / 6]SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
hail: warning: while importing:
    hdfs://seqr-db1/user/cseed/INMR_v9.vep.vcf.bgz
  filtered 9364 genotypes:
    1462 times: sum(AD) > DP
    7274 times: GQ != difference of two smallest PL entries
    628 times: GQ present but PL missing
hail: info: timing:
  importvcf: 2.020s
  splitmulti: 35.076ms
  write: 1m36.0s
```

Annotate with VEP and other reference data on cray1:

```
 
time hail -b 2 importvcf -n 250 INMR_v9.vcf.bgz \
splitmulti \
vep --block-size 250 --force --config /home/users/cseed/vep.properties \
annotatevariants vds -r va.g1k -i 1kg_wgs_phase3.vds \
annotatevariants vds -r va.exac -i exac_v0.3.1.vds \
annotatevariants vds -r va.clinvar -i clinvar_v2016_08_04.vds \
annotatevariants vds -r va.dbnsfp -i dbNSFP_3.2a_variant.filtered.allhg19_nodup.vds \
printschema count \
write -o INMR_v9.vds

```


Annotate with just VEP on cray1:

```
[weisburd@nid00014 data]$ time hail -b 2 importvcf -n 250 importvcf INMR_v9.vcf.bgz splitmulti vep --block-size 250 --force --config /home/users/cseed/vep.properties printschema count write -o INMR_v9.vds

hail: info: running: importvcf INMR_v9.subset.vcf.bgz
hail: info: running: splitmulti
hail: info: running: vep --block-size 250 --force --config /home/users/cseed/vep.properties
hail: info: vep: annotated 1991 variants
hail: info: running: printschema
Global annotation schema:
global: Empty

Sample annotation schema:
sa: Empty

Variant annotation schema:
va: Struct {
    rsid: String,
    qual: Double,
    filters: Set[String],
    pass: Boolean,
    info: Struct {
        AC: Array[Int],
        AF: Array[Double],
        AN: Int,
        BaseQRankSum: Double,
        CCC: Int,
        ClippingRankSum: Double,
        DB: Boolean,
        DP: Int,
        DS: Boolean,
        END: Int,
        FS: Double,
        GQ_MEAN: Double,
        GQ_STDDEV: Double,
        HWP: Double,
        HaplotypeScore: Double,
        InbreedingCoeff: Double,
        MLEAC: Array[Int],
        MLEAF: Array[Double],
        MQ: Double,
        MQ0: Int,
        MQRankSum: Double,
        NCC: Int,
        NEGATIVE_TRAIN_SITE: Boolean,
        POSITIVE_TRAIN_SITE: Boolean,
        QD: Double,
        ReadPosRankSum: Double,
        VQSLOD: Double,
        culprit: String
    },
    aIndex: Int,
    wasSplit: Boolean,
    vep: Struct {
        assembly_name: String,
        allele_string: String,
        ancestral: String,
        colocated_variants: Array[Struct {
            aa_allele: String,
            aa_maf: Double,
            afr_allele: String,
            afr_maf: Double,
            allele_string: String,
            amr_allele: String,
            amr_maf: Double,
            clin_sig: Array[String],
            end: Int,
            eas_allele: String,
            eas_maf: Double,
            ea_allele: String,
            ea_maf: Double,
            eur_allele: String,
            eur_maf: Double,
            exac_adj_allele: String,
            exac_adj_maf: Double,
            exac_allele: String,
            exac_afr_allele: String,
            exac_afr_maf: Double,
            exac_amr_allele: String,
            exac_amr_maf: Double,
            exac_eas_allele: String,
            exac_eas_maf: Double,
            exac_fin_allele: String,
            exac_fin_maf: Double,
            exac_maf: Double,
            exac_nfe_allele: String,
            exac_nfe_maf: Double,
            exac_oth_allele: String,
            exac_oth_maf: Double,
            exac_sas_allele: String,
            exac_sas_maf: Double,
            id: String,
            minor_allele: String,
            minor_allele_freq: Double,
            phenotype_or_disease: Int,
            pubmed: Array[Int],
            sas_allele: String,
            sas_maf: Double,
            somatic: Int,
            start: Int,
            strand: Int
        }],
        context: String,
        end: Int,
        id: String,
        input: String,
        intergenic_consequences: Array[Struct {
            allele_num: Int,
            consequence_terms: Array[String],
            impact: String,
            minimised: Int,
            variant_allele: String
        }],
        most_severe_consequence: String,
        motif_feature_consequences: Array[Struct {
            allele_num: Int,
            consequence_terms: Array[String],
            high_inf_pos: String,
            impact: String,
            minimised: Int,
            motif_feature_id: String,
            motif_name: String,
            motif_pos: Int,
            motif_score_change: Double,
            strand: Int,
            variant_allele: String
        }],
        regulatory_feature_consequences: Array[Struct {
            allele_num: Int,
            biotype: String,
            consequence_terms: Array[String],
            impact: String,
            minimised: Int,
            regulatory_feature_id: String,
            variant_allele: String
        }],
        seq_region_name: String,
        start: Int,
        strand: Int,
        transcript_consequences: Array[Struct {
            allele_num: Int,
            amino_acids: String,
            biotype: String,
            canonical: Int,
            ccds: String,
            cdna_start: Int,
            cdna_end: Int,
            cds_end: Int,
            cds_start: Int,
            codons: String,
            consequence_terms: Array[String],
            distance: Int,
            domains: Array[Struct {
                db: String,
                name: String
            }],
            exon: String,
            gene_id: String,
            gene_pheno: Int,
            gene_symbol: String,
            gene_symbol_source: String,
            hgnc_id: Int,
            hgvsc: String,
            hgvsp: String,
            hgvs_offset: Int,
            impact: String,
            intron: String,
            lof: String,
            lof_flags: String,
            lof_filter: String,
            lof_info: String,
            minimised: Int,
            polyphen_prediction: String,
            polyphen_score: Double,
            protein_end: Int,
            protein_start: Int,
            protein_id: String,
            sift_prediction: String,
            sift_score: Double,
            strand: Int,
            swissprot: String,
            transcript_id: String,
            trembl: String,
            uniparc: String,
            variant_allele: String
        }],
        variant_class: String
    }
}
hail: info: running: count
hail: info: count:
  nSamples               365
  nVariants            1,991
hail: info: running: write -o INMR_v9.subset.vds
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
hail: warning: while importing:
    hdfs://192.168.0.1:8020/user/weisburd/INMR_v9.subset.vcf.bgz
  filtered 10 genotypes:
    1 time: sum(AD) > DP
    9 times: GQ != difference of two smallest PL entries
hail: info: timing:
  importvcf: 652.487ms
  splitmulti: 9.908ms
  vep: 5m36.5s
  printschema: 5.991ms
  count: 85.711ms
  write: 3.127s
```
