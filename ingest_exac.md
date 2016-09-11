**INPUT:** ExAC.r0.3.1.sites.vep.vcf.bgz

**HAIL COMMAND:**  
```
time hail -b 2 --parquet-compression snappy \
importvcf -n 250 file:///mnt/lustre/weisburd/data/reference_data/exac/ExAC.r0.3.1.sites.vep.vcf.bgz \
annotatevariants expr -c '
    va.info.CSQ = NA: Boolean,
    va.info.AC_POPMAX = va.info.AC_POPMAX.map(x => if(x == "NA") NA:Int else x.toInt), 
    va.info.AN_POPMAX = va.info.AN_POPMAX.map(x => if(x == "NA") NA:Int else x.toInt)' \
splitmulti \
printschema \
write -o file:///mnt/lustre/weisburd/data/reference_data/exac/ExAC.r0.3.1.sites.vds
```

**LOG:**
```
hail: info: running: annotatevariants expr -c '
    va.info.CSQ = NA: Boolean,
    va.info.AC_POPMAX = va.info.AC_POPMAX.map(x => if(x == "NA") NA:Int else x.toInt),
    va.info.AN_POPMAX = va.info.AN_POPMAX.map(x => if(x == "NA") NA:Int else x.toInt)'
hail: info: partitions: 250, 249
hail: info: running: splitmulti
hail: info: partitions: 250, 249
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
        AC_AFR: Array[Int],
        AC_AMR: Array[Int],
        AC_Adj: Array[Int],
        AC_EAS: Array[Int],
        AC_FIN: Array[Int],
        AC_Hemi: Array[Int],
        AC_Het: Array[Int],
        AC_Hom: Array[Int],
        AC_NFE: Array[Int],
        AC_OTH: Array[Int],
        AC_SAS: Array[Int],
        AF: Array[Double],
        AN: Int,
        AN_AFR: Int,
        AN_AMR: Int,
        AN_Adj: Int,
        AN_EAS: Int,
        AN_FIN: Int,
        AN_NFE: Int,
        AN_OTH: Int,
        AN_SAS: Int,
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
        Hemi_AFR: Array[Int],
        Hemi_AMR: Array[Int],
        Hemi_EAS: Array[Int],
        Hemi_FIN: Array[Int],
        Hemi_NFE: Array[Int],
        Hemi_OTH: Array[Int],
        Hemi_SAS: Array[Int],
        Het_AFR: Array[Int],
        Het_AMR: Array[Int],
        Het_EAS: Array[Int],
        Het_FIN: Array[Int],
        Het_NFE: Array[Int],
        Het_OTH: Array[Int],
        Het_SAS: Array[Int],
        Hom_AFR: Array[Int],
        Hom_AMR: Array[Int],
        Hom_EAS: Array[Int],
        Hom_FIN: Array[Int],
        Hom_NFE: Array[Int],
        Hom_OTH: Array[Int],
        Hom_SAS: Array[Int],
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
        culprit: String,
        DP_HIST: Array[String],
        GQ_HIST: Array[String],
        DOUBLETON_DIST: Array[String],
        AC_MALE: Array[String],
        AC_FEMALE: Array[String],
        AN_MALE: String,
        AN_FEMALE: String,
        AC_CONSANGUINEOUS: Array[String],
        AN_CONSANGUINEOUS: String,
        Hom_CONSANGUINEOUS: Array[String],
        CSQ: Boolean,
        AC_POPMAX: Array[Int],
        AN_POPMAX: Array[Int],
        POPMAX: Array[String],
        clinvar_measureset_id: Array[String],
        clinvar_conflicted: Array[String],
        clinvar_pathogenic: Array[String],
        clinvar_mut: Array[String],
        K1_RUN: Array[String],
        K2_RUN: Array[String],
        K3_RUN: Array[String],
        ESP_AF_POPMAX: Array[String],
        ESP_AF_GLOBAL: Array[String],
        ESP_AC: Array[String],
        KG_AF_POPMAX: Array[String],
        KG_AF_GLOBAL: Array[String],
        KG_AC: Array[String]
    },
    aIndex: Int,
    wasSplit: Boolean
}
hail: info: running: write -o file:///mnt/lustre/weisburd/data/reference_data/exac/ExAC.r0.3.1.sites.vds
[Stage 2:======================================================>(249 + 1) / 250]SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
hail: info: while importing:
    file:/mnt/lustre/weisburd/data/reference_data/exac/ExAC.r0.3.1.sites.vep.vcf.bgz  import clean
hail: info: timing:
  importvcf: 9.564s
  annotatevariants expr: 136.275ms
  splitmulti: 17.146ms
  printschema: 4.718ms
  write: 1m46.4s
I0910 23:22:33.524613 33313 sched.cpp:1771] Asked to stop the driver
I0910 23:22:33.524718 33424 sched.cpp:1040] Stopping framework '0233fcf9-88ce-407f-8ed5-b015adf9b59c-2824'

real   	2m1.100s
user   	0m37.570s
sys    	0m4.719s
```
