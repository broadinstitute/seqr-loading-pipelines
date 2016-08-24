**INPUT:** 
Latest `clinvar.tsv.gz` from the https://github.com/macarthur-lab/clinvar repo

**HAIL COMMAND:**  
```
dmz-seqr-db1:~ 1010 0 $ time hail importannotations \
table -t 'pos: Int, pathogenic: Int, conflicted: Int, measureset_id: Int' -e 'Variant(chrom,pos,ref,alt)' \
clinvar_2016_08_04.tsv.gz \
splitmulti \
write -o clinvar_v2016_08_04.vds

hail: info: running: importannotations table -t 'pos: Int, pathogenic: Int, conflicted: Int, measureset_id: Int' -e Variant(chrom,pos,ref,alt) clinvar.tsv.gz
hail: info: Reading table with no type imputation
  Loading column `chrom' as type `String' (type not specified)
  Loading column `pos' as type `Int' (user-specified)
  Loading column `ref' as type `String' (type not specified)
  Loading column `alt' as type `String' (type not specified)
  Loading column `mut' as type `String' (type not specified)
  Loading column `measureset_id' as type `Int' (user-specified)
  Loading column `symbol' as type `String' (type not specified)
  Loading column `clinical_significance' as type `String' (type not specified)
  Loading column `review_status' as type `String' (type not specified)
  Loading column `hgvs_c' as type `String' (type not specified)
  Loading column `hgvs_p' as type `String' (type not specified)
  Loading column `all_submitters' as type `String' (type not specified)
  Loading column `all_traits' as type `String' (type not specified)
  Loading column `all_pmids' as type `String' (type not specified)
  Loading column `pathogenic' as type `Int' (user-specified)
  Loading column `conflicted' as type `Int' (user-specified)

hail: info: running: write -o clinvar_v2016_07_07.vds
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
hail: info: timing:
  importannotations table: 2.662s
  write: 18.668s
  
```
