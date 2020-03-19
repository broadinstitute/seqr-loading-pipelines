
# Luigi

## Local
### Install
```
$ pip install -r requirements.txt
```

### Tests
```
$ pip install nose
$ PYTHONPATH=.. nosetests
```

### Running Locally

```
$ python3 seqr_loading.py SeqrMTToESTask --local-scheduler \
    --source-paths  gs://seqr-datasets/GRCh37/1kg/1kg.vcf.gz \
    --genome-version 37 \
    --sample-type WES \
    --dest-path gs://seqr-datasets/GRCh37/1kg/1kg.mt \
    --reference-ht-path  gs://seqr-reference-data/GRCh37/all_reference_data/combined_reference_data_grch37.ht \
    --clinvar-ht-path gs://seqr-reference-data/GRCh37/clinvar/clinvar.GRCh37.ht \
    --es-host 100.15.0.1 \
    --es-index new-es-index-name \ 
    --es-index-min-num-shards 3
```
Run `PYTHONPATH=.. python3 seqr_loading.py SeqrMTToESTask --help` for a description of these args.
Optionally, any of these parameters can also be set via config file instead of on the command line. 
`configs/luigi.cfg` provides an example. The `LUIGI_CONFIG_PATH` environment variable can be used to specify the config file path:
```
LUIGI_CONFIG_PATH=configs/seqr-loading-local.cfg
```

## Running on GCE Dataproc
### Create a cluster

Installing [hail](http://hail.is) also installs the hailctl utility which makes it easy to start up Dataproc clusters 
and submitting jobs to them.   
```
$ hailctl dataproc start \
    --pkgs luigi,google-api-python-client \
    --vep GRCh37 \
    --max-idle 30m \
    --num-workers 2 \
    --num-preemptible-workers 12 \
    seqr-loading-cluster
```

### Run

This command is identical to the one under Running Locally, except the script is submitted to Dataproc. 
```
$ hailctl dataproc submit seqr-loading-cluster \
    seqr_loading.py --pyfiles "lib,../hail_scripts" \
    SeqrMTToESTask --local-scheduler \
    --source-paths gs://seqr-datasets/GRCh37/1kg/1kg.vcf.gz \
    --genome-version 37 \
    --sample-type WES \
    --dest-path gs://seqr-datasets/GRCh37/1kg/1kg.mt \
    --reference-ht-path  gs://seqr-reference-data/GRCh37/all_reference_data/combined_reference_data_grch37.ht \
    --clinvar-ht-path gs://seqr-reference-data/GRCh37/clinvar/clinvar.GRCh37.ht \
    --es-host 100.15.0.1 \
    --es-index new-es-index-name \ 
    --es-index-min-num-shards 3
```
