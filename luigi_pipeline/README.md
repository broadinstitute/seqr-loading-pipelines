
# Luigi Work in Progress

## Local
### Install
```
$ pip install -r requirements.txt
```

### Tests
```
$ pip install nose
$ nosetests
```

### Run
```
$ LUIGI_CONFIG_PATH=configs/seqr-loading-local.cfg python seqr_loading.py SeqrMTToESTask --local-scheduler
```

## GCE
### Setup
- Install https://github.com/Nealelab/cloudtools
```
$ cluster start seqr-loading-dev --num-workers 2 --pkgs luigi,google-api-python-client --max-idle 60m
```

### Run
```
$ cluster submit seqr-loading-dev seqr_loading.py --pyfiles lib --files configs/luigi.cfg --args "SeqrMTToESTask --local-scheduler"
```
