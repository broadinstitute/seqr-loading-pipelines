I installed a development version of Hail at
`seqr-db1:/home/cseed/bin/hail-dev`.  I imported INMR_v9 with the
following simple ingest pipeline.

0.  I loaded the ExAC sites file and INMR_v9 into HDFS with something like:

```
cat file | ssh seqr-db1 'hdfs dfs -put - file
```

0.  Import the ExAC sites file into Hail.  This discards the VEP
annotation which is large and not easily interpretable by Hail in the
VCF format.

```
hail-dev --parquet-compression snappy \
  importvcf -n 48 ExAC.r0.3.sites.vep.vcf.bgz \
  annotatevariants expr -c 'va.info.CSQ = NA: Boolean' \
  splitmulti \
  write -o ExAC.sites.vds

  importvcf: 3.810s
  splitmulti: 30.335ms
  write: 8m51.4s
```

0. Import INMR_v9:

```
hail-dev \
  importvcf -n 48 INMR_v9.vep.vcf.bgz \
  splitmulti \
  write -o INMR_v9.vds

  importvcf: 3.630s
  splitmulti: 22.637ms
  write: 1m54.4s
```

0.  Create a test Solr collection and export to Solr.  I'm exporting
the variant keys (contig, start, ref, alt) and sample and exac_af as
well as the num_alt, gq and ab (allele balance) per genotype.  As you
can see, all of this is completely customizable.  Annotating with ExAC
and exporting to Solr took <15m.

```
(cd /local/software/solr-6.0.1; \
  ./bin/solr delete -c test && \
  ./bin/solr create_collection -c test -shards 3) && \
hail-dev \
  read -i INMR_v9.vds \
  annotatevariants vds -r va.exac -i ExAC.sites.vds \
  exportvariantssolr -c test -v 'contig = v.contig,
    start = v.start,
    ref = v.ref,
    alt = v.alt,
    sample_af = va.info.AF[va.aIndex],
    exac_af = va.exac.info.AF[va.exac.aIndex]' \
  -g 'num_alt = g.nNonRefAlleles,
    gq = g.gq,
    ab = let s = g.ad.sum
         in if (s == 0 || !g.isHet)
	   NA: Float
	 else
	   (g.ad[0] / s).toFloat' \
  -z 'seqr-db1:2181,seqr-db2:2181,seqr-db3:2181'

  read: 7.446s
  annotatevariants vds: 2.726s
  exportvariantssolr: 14m38.3s
```

The origin INMR_v9 VCF is 746.1MB.

```
$ hdfs dfs -ls -h INMR_v9.vep.vcf.bgz
-rw-r--r--   1 cseed supergroup    746.1 M 2016-06-20 11:04 INMR_v9.vep.vcf.bgz
```

The Solr shards are 598MB, 521MB and 645MB:

```
seqr-db1$ du -sh /local/solr/test_shard3_replica1
645M	/local/solr/test_shard3_replica1
seqr-db2$ du -sh /local/solr/test_shard1_replica1
598M	/local/solr/test_shard1_replica1
seqr-db3$ du -sh /local/solr/test_shard2_replica1
521M	/local/solr/test_shard2_replica1
```

This means the total Solr storage for INMR_v9 is ~2.4x larger than the
original VCF.

You can query the test collection
[here](http://seqr-db1:8983/solr/#/test/query).
