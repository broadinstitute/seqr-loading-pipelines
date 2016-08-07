
**HAIL COMMAND:**
```
## import data
time hail \
  read -i INMR_v9.subset.vds \
  annotatevariants vds -r va.g1k -i 1kg_wgs_phase3.vds \
  annotatevariants vds -r va.exac -i exac_v0.3.1.vds \
  annotatevariants vds -r va.clinvar -i clinvar_v2016_07_07.vds \
  annotatevariants vds -r va.dbnsfp -i dbNSFP_3.2a_variant.filtered.allhg19_nodup.vds \
  printschema \
  exportvariantscass -k test -t test -a seqr-db1 -v 'contig = v.contig,
    start = v.start,
    ref = v.ref,
    alt = v.alt,
    pass = va.pass,
    filter = if(va.pass) "PASS" else va.filters.mkString(","),
    g1k_wgs_phase3_global_AF = va.g1k.info.AF[va.g1k.aIndex],
    g1k_wgs_phase3_popmax_AF = va.g1k.info.POPMAX_AF,
    exac_v3_global_AF = va.exac.info.AF[va.exac.aIndex],
    exac_v3_popmax_AF = va.exac.info.POPMAX,
    sample_af = va.info.AF[va.aIndex], 
    dataset_id = "INMR",
    dataset_version = "2016_04_12",
    dataset_type = "wex"' \
  -g 'num_alt = g.nNonRefAlleles,
    gq = g.gq,
    ab = let s = g.ad.sum
         in if (s == 0 || !g.isHet)
       NA: Float
     else
       (g.ad[0] / s).toFloat' 
```

