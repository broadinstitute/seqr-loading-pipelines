**INPUT:** 
Latest `clinvar.tsv.gz` from the https://github.com/macarthur-lab/clinvar repo

**HAIL COMMAND:**  
```
time hail importannotations \
table -t 'pos: Int, pathogenic: Int, conflicted: Int, measureset_id: Int, gold_stars: Int' -e 'Variant(chrom,pos,ref,alt)' \
file:///mnt/lustre/weisburd/data/reference_data/clinvar/clinvar_2016_09_01.tsv.gz \
splitmulti \
write -o file:///mnt/lustre/weisburd/data/reference_data/clinvar/clinvar_v2016_09_01.vds
```

**HAIL LOG:**
```
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
