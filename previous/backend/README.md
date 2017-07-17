Scripts to set up a Seqr backend.

 - Create a Google Cloud VM instance running Ubuntu 16.10 and a local SSD scratch disk using SCSI.  (We should switch to NVMe.  I haven't tested it yet).

 - Connect to the instance.  Clone this repo.

 - Run `bash seqr-hail-db/backend/init-vm.sh`.  This will mount the SSD on `/data` and install Hail, Solr and Cassandra and any dependencies.

 - Log out and back in.  `init-vm.sh` sets up some environment variables.

To populate a test dataset:

 - Run `bash bash seqr-hail-db/backend/start-db.sh` to start Solr and Cassandra.

 - Run `bash seqr-hail-db/backend/test-create-db.sh`.  This will create a Solr collection `test` and a Cassandra table `test.test`.

 - Export some data: `python seqr-hail-db/backend/test-export.py $HOME/hail/src/test/resources/sample.vcf`.  This will export `sample.vcf` to Solr, Cassandra and write the VDS `/data/seqr/datasets/test.vds`.  `sample.vcf` has 100 samples and 346 variants.  

 - Run `python seqr-hail-db/backend/test-seqrd.py` to start the Seqr server.  It will run in the foreground.

 - Make a query!

   ```
   $ time curl http://localhost:6060 -d '{
     "limit": 50,
     "page": 1,
     "genotype_filters": {
     "C1046::HG02025": { "num_alt": { "eq": 1 } },
     "HG00622": { "num_alt": { "eq": 1 } } } }'
   {"is_error":false,"variants":[{"HG00629":{"dp":58,...
   real	0m0.314s
   user	0m0.004s
   sys	0m0.000s
   ```
