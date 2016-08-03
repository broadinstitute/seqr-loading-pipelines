
INPUT: ALL.wgs.phase3_shapeit2_mvncall_integrated_v5a.20130502.sites.decomposed.with_popmax.vcf.bgz   (TODO: document how this was made)

hail command:

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
[Stage 0:>                                                         (0 + 6) / 16]

```
