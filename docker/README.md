# seqr pipeline-runner docker image

This folder contains the files necessary to build a docker image that can be used by local seqr installation to run
the seqr loading pipeline.
This image is referenced in the seqr
[docker-compose](https://github.com/broadinstitute/seqr/blob/master/docker-compose.yml)
file, and full instructions for how to run the pipeline in the image can be found in the
[seqr repository](https://github.com/broadinstitute/seqr/blob/master/deploy/LOCAL_INSTALL.md#annotating-and-loading-vcf-callsets).

To build the image, run the following:
```bash
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
docker build . -f docker/Dockerfile --platform=linux/amd64 -t gcr.io/seqr-project/pipeline-runner:gcloud-prod -t gcr.io/seqr-project/pipeline-runner:${TIMESTAMP}
docker push gcr.io/seqr-project/pipeline-runner:gcloud-prod
docker push gcr.io/seqr-project/pipeline-runner:${TIMESTAMP}
```
