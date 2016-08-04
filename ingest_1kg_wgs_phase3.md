
**INPUT:** ALL.wgs.phase3_shapeit2_mvncall_integrated_v5a.20130502.sites.vcf.bgz  

**hail command:**

```
dmz-seqr-db1:~ 1005 130 $ hail \
importvcf  ALL.wgs.phase3_shapeit2_mvncall_integrated_v5a.20130502.sites.vcf.bgz \
splitmulti \
annotatevariants expr -c 'va.info.POPMAX_AF = [va.info.EAS_AF[0], va.info.EUR_AF[0], va.info.AFR_AF[0], va.info.AMR_AF[0], va.info.SAS_AF[0]].max' \
annotatevariants expr -c 'va.info.POPMAX = if (va.info.POPMAX_AF==0) "NA" else if (va.info.POPMAX_AF==va.info.EAS_AF[0]) "EAS" else if (va.info.POPMAX_AF==va.info.EUR_AF[0]) "EUR" else if (va.info.POPMAX_AF==va.info.AFR_AF[0]) "AFR" else if (va.info.POPMAX_AF==va.info.AMR_AF[0]) "AMR" else if (va.info.POPMAX_AF==va.info.SAS_AF[0]) "SAS" else "ERROR:UNEXPECTED_POPMAX"' \
printschema \
write -o 1kg_wgs_phase3.vds

hail: info: running: importvcf ALL.wgs.phase3_shapeit2_mvncall_integrated_v5a.20130502.sites.vcf.bgz
hail: info: running: splitmulti
hail: info: running: annotatevariants expr -c 'va.info.POPMAX_AF = [va.info.EAS_AF[0], va.info.EUR_AF[0], va.info.AFR_AF[0], va.info.AMR_AF[0], va.info.SAS_AF[0]].max'
hail: info: running: annotatevariants expr -c 'va.info.POPMAX = if (va.info.POPMAX_AF==0) "NA" else if (va.info.POPMAX_AF==va.info.EAS_AF[0]) "EAS" else if (va.info.POPMAX_AF==va.info.EUR_AF[0]) "EUR" else if (va.info.POPMAX_AF==va.info.AFR_AF[0]) "AFR" else if (va.info.POPMAX_AF==va.info.AMR_AF[0]) "AMR" else if (va.info.POPMAX_AF==va.info.SAS_AF[0]) "SAS" else "ERROR:UNEXPECTED_POPMAX"'
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
        POPMAX_AF: Double,
        POPMAX: String
    },
    aIndex: Int,
    wasSplit: Boolean
}
hail: info: running: write -o 1kg_wgs_phase3.vds
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
hail: warning: while importing:
    hdfs://seqr-db1/user/weisburd/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5a.20130502.sites.vcf.bgz
  filtered 59537 variants:
    59537 times: Variant is symbolic
hail: warning: while importing:
    hdfs://seqr-db1/user/weisburd/ALL.wgs.phase3_shapeit2_mvncall_integrated_v5a.20130502.sites.vcf.bgz
  filtered 59537 variants:
    59537 times: Variant is symbolic
hail: info: timing:
  importvcf: 2.071s
  splitmulti: 32.967ms
  annotatevariants expr: 600.736ms
  annotatevariants expr: 380.029ms
  printschema: 16.300ms
  write: 37m6.8s
```
