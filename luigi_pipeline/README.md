
# Luigi Work in Progress

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

### Run
```
$ LUIGI_CONFIG_PATH=configs/seqr-loading-local.cfg python seqr_loading.py SeqrMTToESTask --local-scheduler
```

## GCE
### Setup
- Install https://github.com/Nealelab/cloudtools
```
$ cluster start seqr-loading-dev --num-workers 2 --pkgs luigi,google-api-python-client --max-idle 60m --vep
```

### Run
```
$ cluster submit seqr-loading-dev seqr_loading.py --pyfiles lib,../hail_scripts --files configs/luigi.cfg --args "SeqrMTToESTask --local-scheduler"
```

### Run with dataproc and ES wrapper
We use a ported `load_dataset_v02.py` wrapper to spin up dataproc and ES nodes.
Sample run:
```
PYTHONPATH=.. ./load_dataset_v02.py --num-workers 2 --num-preemptible-workers 0 --genome-version 37 test  --project-guid test --use-temp-loading-nodes --k8s-cluster-name my-k8s-test --cluster-name dataproc-test --num-temp-loading-nodes 2 --create-persistent-es-nodes
```
