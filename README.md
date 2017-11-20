This README describes how run the hail scripts in this repo to pre-process variant callsets and export them to elasticsearch. 
It assumes you already have a running elasticsearch instance. If not, see
https://github.com/macarthur-lab/elasticsearch-kubernetes-cluster for deploying a stand-alone elasticsearch cluster, or https://github.com/macarthur-lab/seqr for deploying one as part of seqr.


Scripts
-------

Scripts to create/modify/delete dataproc clusters:

* `create_cluster_GRCh37.py` - creates a dataproc cluster that has VEP with a GRCh37 cache pre-installed. This allows hail pipelines to to use `vds.vep(..)` to run VEP on GRCh37-aligned datasets. 
* `create_cluster_GRCh38.py` - creates a dataproc cluster that has VEP with a GRCh38 cache pre-installed. This allows hail pipelines to to use `vds.vep(..)` to run VEP on GRCh38-aligned datasets. 
* `create_cluster_without_VEP.py` creates a dataproc cluster without installing VEP, so `vds.vep(..)` won't work. 

* `create_cluster_notebook.py` creates a cluster that supports an ipython notebook. 
* `connect_to_cluster.py` connects to a cluster that was created by `create_cluster_notebook.py`, and re-opens ipython dashboard in the browser.

* `submit.py` submits a python/hail script to the cluster

* `list_clusters.py` prints the names of all existing dataproc clusters in the project
* `list_jobs.py` lists all active dataproc jobs
* `delete_cluster.py` deletes a specific dataproc cluster
* `delete_job.py` / `kill_job.py` kills a specific hail job
* `describe_cluster.py` prints gcloud details on a specific dataproc cluster
* `describe_job.py` prints details on a specific dataproc job

Hail pipelines:
* `entire_vds_pipeline.py` annotation and pre-processing pipeline for GRCh37 and GRCh38 rare disease callsets
* `run_vep.py` run VEP on a vcf or vds and write the result to a .vds. WARNING: this must run on a cluster created with either `create_cluster_GRCh37.py` or `create_cluster_GRCh38.py`, depending on the genome version of the dataset being annotated.
- `export_gnomad_to_ES.py` - joins gnomad exome and genome datasets into a structure that contains the info used in the gnomAD browser, and exports this to elasticsearch.

Other hail pipelines:
* `convert_tsv_to_vds.py` converts a .tsv table to a VDS by allowing the user to specify the chrom, pos, ref, alt column names
* `convert_vcf_to_vds.py` import a vcf and writes it out as a vds
* `create_subset.py` subsets a vcf or vds to a specific chromosome or locus - useful for creating small datasets for testing. 
* `print_vds_schema.py` print out the vds variant schema
* `print_vds_stats.py`  print out vds stats such as the schema, variant count, etc.
* `export_vds_to_tsv.py`  export a subset of vds variants to a .tsv for inspection


Example:
    ./create_cluster_GRCh37.py 
    ./submit.sh run_vep.py gs://<input dataset path>  gs://<output path>

Kubernetes Resources
--------------------

- Official Kuberentes User Guide:  https://kubernetes.io/docs/user-guide/
- 15 Kubernetes Features in 15 Minutes: https://www.youtube.com/watch?v=o85VR90RGNQ
- Kubernetes: Up and Running: https://www.safaribooksonline.com/library/view/kubernetes-up-and/9781491935668/
- The Children's Illustrated Guide to Kubernetes: https://deis.com/blog/2016/kubernetes-illustrated-guide/

