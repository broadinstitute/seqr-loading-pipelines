cqlsh seqr-db1 -k test -e 'select chrom, start, end, ref, alt from test limit 1'
cqlsh seqr-db1 -k test -e 'drop table test'
cqlsh seqr-db1 -k test -e 'create table test (chrom text, start int, ref text, alt text, PRIMARY KEY ((chrom, start), ref, alt))'
cqlsh seqr-db1 -k test -e 'select chrom, start, end, ref, alt from test limit 1'