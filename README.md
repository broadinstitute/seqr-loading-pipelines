# Seqr Loading Pipeline

[![Code Build](https://github.com/broadinstitute/seqr-loading-pipelines/actions/workflows/unit-tests.yml/badge.svg?branch=main)](https://github.com/broadinstitute/seqr-loading-pipelines/actions/workflows/unit-tests.yml)
[![Docker Build](https://github.com/broadinstitute/seqr-loading-pipelines/actions/workflows/prod-release.yml/badge.svg?branch=main)](https://github.com/broadinstitute/seqr-loading-pipelines/actions/workflows/prod-release.yml)

*This repository contains pipelines and infrastructure for loading genomic data from VCF -> ClickHouse to support queries by _seqr_ application*

---

## 📁 Repository Structure

### `api/`
Contains the interface layer to the _seqr_ application. 
- `api/model.py` defines pydantic models for the REST interface.
- `api/app.py` specifies an `aiohttp` webserver that handles load data requests. 

### `bin/`
Scripts or command-line utilities used for setup or task execution.
- `bin/pipeline_worker.py` — manages asynchronous jobs requested by _seqr_.

### `deploy/`
Dockerfiles for the loading pipeline itself & any annotation utilities.
Kubernetes manifests are managed separately in [seqr-helm](https://github.com/broadinstitute/seqr-helm/tree/main/charts/pipeline-runner)

### `lib/`
Core logic and shared libraries.  
- `annotations` defines hail logic to re-format and standardize fields.
- `methods` wraps hail-defined genomics methods for QC.
- `misc` contains single modules with defined utilities.
	- `misc/clickhouse` hosts the logic that manages the parquet ingestion into ClickHouse itself.
- `core` defines key constants/enums/config.
- `reference_datasets` manages parsing of raw reference sources into hail tables.
- `tasks` specifies the Luigi defined pipeline.  Note that Luigi pipelines are defined by their requirements, so
the pipeline is defined, effectively, in reverse.
	- `WriteSuccessFile` is the last task, defining a `requires()` method that runs the pipeline either locally or on scalable compute.
	- `WriteImportedCallset` is the first task, importing a VCF into a Hail Matrix table, an "imported callset".
- `test` holds a few utilities used by the tests, which are dispersed throughout the rest of the repository.
- `paths.py` defines paths for all intermediate and output files of the pipeline.

### `var/`
Static configuration and test files.

---

## ⚙️ Setup for local development
The production pipeline runs with python `3.11`.

### Clone the repo and install python requirements.
```bash
git clone https://github.com/broadinstitute/seqr-loading-pipelines.git
cd seqr-loading-pipelines
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### [Install](https://clickhouse.com/docs/getting-started/quick-start/oss) & start ClickHouse with provided test configuration:
```bash
curl https://clickhouse.com/ | sh
./clickhouse server --config-file=./seqr-loading-pipelines/v03_pipeline/var/clickhouse_config/test-clickhouse.xml
```

### [Run the tests](https://github.com/broadinstitute/seqr-loading-pipelines/blob/main/.github/workflows/unit-tests.yml#L66-L73)

### Run a specific test
```bash
nosetests v03_pipeline/lib/misc/math_test.py
```

### Run formatting and linting
```bash
ruff format .
ruff check .
```


## 🚶‍♂️Walkthrough of the Pipeline

