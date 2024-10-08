FROM docker:dind as BUILD
FROM hailgenetics/hail:0.2.132-py3.10
COPY --from=BUILD /usr/local/bin/docker /usr/local/bin/docker
LABEL maintainer="Broad TGG"

RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH $PATH:/root/google-cloud-sdk/bin

WORKDIR /v03_pipeline

## Copy & Install Application Deps 
COPY requirements.txt .
COPY requirements-prod.txt .
RUN python3 -m pip install --no-cache-dir -r ./requirements.txt -r requirements-prod.txt

# Application Code
COPY v03_pipeline/api api
COPY v03_pipeline/bin bin
COPY v03_pipeline/lib lib
COPY v03_pipeline/migrations migrations

# Special paths
COPY v03_pipeline/var/spark_config/spark-defaults.conf /usr/local/lib/python3.10/dist-packages/pyspark/conf/spark-defaults.conf
COPY v03_pipeline/bin/vep /vep


WORKDIR /
EXPOSE 6000 8082
CMD ["python3", "-m", "v03_pipeline.api"]
