
```
dmz-seqr-db1:/local 1030 0 $ hail importvcf  --store-gq ../cseed/INMR_v9.vep.vcf.bgz splitmulti write -o INMR_v9.vds
hail: info: running: importvcf --store-gq ../cseed/INMR_v9.vep.vcf.bgz
hail: info: running: splitmulti
hail: info: running: write -o INMR_v9.vds
[Stage 0:=======================================>                   (4 + 2) / 6]SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
hail: warning: while importing:
    hdfs://seqr-db1/user/cseed/INMR_v9.vep.vcf.bgz
  filtered 1462 genotypes:
    1462 times: sum(AD) > DP
hail: info: timing:
  importvcf: 2.085s
  splitmulti: 39.870ms
  write: 1m36.3s
```
