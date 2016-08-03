
*INPUT:* ALL.wgs.phase3_shapeit2_mvncall_integrated_v5a.20130502.sites.decomposed.with_popmax.vcf.bgz  
(TODO: document how this was made)

*hail command:*

```
dmz-seqr-db1:~ 1005 130 $ time hail importvcf ALL.wgs.phase3_shapeit2_mvncall_integrated_v5a.20130502.sites.decomposed.with_popmax.vcf.bgz splitmulti printschema write -o 1kg_wgs_phase3.vds
hail: info: running: importvcf ALL.wgs.phase3_shapeit2_mvncall_integrated_v5a.20130502.sites.decomposed.with_popmax.vcf.bgz
hail: info: running: splitmulti
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
        CIEND: Array[Int],
        CIPOS: Array[Int],
        CS: String,
        END: Int,
        IMPRECISE: Boolean,
        MC: Array[String],
        MEINFO: Array[String],
        MEND: Int,
        MLEN: Int,
        MSTART: Int,
        SVLEN: Array[Int],
        SVTYPE: String,
        TSD: String,
        AC: Array[Int],
        AF: Array[Double],
        NS: Int,
        AN: Int,
        EAS_AF: Array[Double],
        EUR_AF: Array[Double],
        AFR_AF: Array[Double],
        AMR_AF: Array[Double],
        SAS_AF: Array[Double],
        DP: Int,
        AA: String,
        VT: Array[String],
        EX_TARGET: Boolean,
        MULTI_ALLELIC: Boolean,
        OLD_MULTIALLELIC: String
    },
    aIndex: Int,
    wasSplit: Boolean
}
hail: info: running: write -o 1kg_wgs_phase3.vds
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
hail: warning: while importing:
    hdfs://seqr-db1/user/weisburd/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5a.20130502.sites.decomposed.with_popmax.vcf.bgz
  filtered 63065 variants:
    63065 times: Variant is symbolic
hail: warning: while importing:
    hdfs://seqr-db1/user/weisburd/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5a.20130502.sites.decomposed.with_popmax.vcf.bgz
  filtered 63065 variants:
    63065 times: Variant is symbolic
hail: info: timing:
  importvcf: 2.396s
  splitmulti: 48.432ms
  printschema: 16.018ms
  write: 31m8.4s
  
real	31m19.637s

```
