The hail scripts in this repo can be used to pre-process variant callsets and export them to elasticsearch. 


Scripts
-------

**Scripts for creating dataproc clusters:**

* `create_cluster_GRCh37.py` - creates a dataproc cluster that has VEP pre-installed with a GRCh37 cache. This allows hail pipelines to to use `vds.vep(..)` to run VEP on GRCh37-aligned datasets. 
* `create_cluster_GRCh38.py` - creates a dataproc cluster that has VEP pre-installed with a GRCh38 cache. This allows hail pipelines to to use `vds.vep(..)` to run VEP on GRCh38-aligned datasets. 
* `create_cluster_without_VEP.py` creates a dataproc cluster without installing VEP, so `vds.vep(..)` won't work. 
* `create_cluster_notebook.py` creates a cluster that allows hail commmands to be run interactively in an ipython notebook. 
* `connect_to_cluster.py` connects to a cluster that was created by `create_cluster_notebook.py`, and re-opens ipython dashboard in the browser.

**Scripts for describing, modifying and deleting dataproc clusters:**

* `list_clusters.py` prints the names of all existing dataproc clusters in the project.
* `list_jobs.py` lists all active dataproc jobs.
* `describe_cluster.py` prints gcloud details on a specific dataproc cluster.
* `describe_job.py` prints details on a specific dataproc job.
* `resize_cluster.py` resize an existing dataproc cluster.
* `delete_cluster.py` deletes a specific dataproc cluster.
* `delete_job.py` / `kill_job.py` kills a specific hail job.

**Script for submitting jobs:**

* `submit.py` submits a python hail script to the cluster.

**Main hail pipelines:**

* `entire_vds_pipeline.py` annotation and pre-processing pipeline for GRCh37 and GRCh38 rare disease callsets.
* `run_vep.py` run VEP on a vcf or vds and write the result to a .vds. WARNING: this must run on a cluster created with either `create_cluster_GRCh37.py` or `create_cluster_GRCh38.py`, depending on the genome version of the dataset being annotated.
- `export_gnomad_to_ES.py` - joins gnomad exome and genome datasets into a structure that contains the info used in the gnomAD browser, and exports this to elasticsearch.

**Other hail pipelines:**

* `create_subset.py` subsets a vcf or vds to a specific chromosome or locus - useful for creating small datasets for testing. 
* `convert_tsv_to_vds.py` converts a .tsv table to a VDS by allowing the user to specify the chrom, pos, ref, alt column names
* `convert_vcf_to_vds.py` import a vcf and writes it out as a vds
* `export_vds_to_tsv.py`  export a subset of vds variants to a .tsv for inspection
* `print_vds_schema.py` print out the vds variant schema
* `print_keytable_schema.py` reads in a tsv and imputes the types. Then prints out the keytable schema.
* `print_vds_stats.py`  print out vds stats such as the schema, variant count, etc.
* `print_elasticsearch_stats.py` connects to an elasticsearch instance and prints current indices and other stats 

*NOTE:* Some of the scripts require a running elasticsearch instance. For deploying a stand-alone elasticsearch cluster see: https://github.com/macarthur-lab/elasticsearch-kubernetes-cluster or for deploying one as part of seqr see: https://github.com/macarthur-lab/seqr


**Examples:**

Run VEP:
```
./create_cluster_GRCh37.py 
./submit.py run_vep.py gs://<dataset path> 
```

Run rare disease callset pipeline:
```    
./create_cluster_GRCh38.py --project=seqr-project cluster1 2 12 ;   # create cluster with 2 persistant, 12 preemptible nodes

./submit.py --cluster cluster1 --project seqr-project ./entire_vds_pipeline.py -g 38 --max-samples-per-index 180 --host $ELASTICSEARCH_HOST_IP --num-shards 12  --project-id my_dataset_name  --sample-type WES  -d GATK_VARIANTS  gs://my-datasets/GRCh38/my_dataset.vcf.gz
```

There's also a shortcut for running the rare disease pipeline which combines the 2 commands above into 1:
```
python load_GRCh38_dataset.py --host $ELASTICSEARCH_HOST_IP --project-id my_dataset_name  --sample-type WES  -d GATK_VARIANTS gs://my-datasets/GRCh38/my_dataset.vcf.gz
```

