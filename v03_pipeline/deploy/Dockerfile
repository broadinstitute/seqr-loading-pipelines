FROM docker:dind as BUILD
FROM hailgenetics/hail:0.2.132-py3.10
COPY --from=BUILD /usr/local/bin/docker /usr/local/bin/docker

LABEL maintainer="Broad TGG"

WORKDIR /v03_pipeline

## Copy & Install Application Deps 
COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r ./requirements.txt

# VEP
COPY v03_pipeline/bin/vep /vep

# Application Code
COPY v03_pipeline/ .

RUN cp ./var/spark_config/spark-defaults.conf /usr/local/lib/python3.10/dist-packages/pyspark/conf/spark-defaults.conf

WORKDIR /
EXPOSE 5000
CMD ["python3", "-m", "v03_pipeline.api"]
