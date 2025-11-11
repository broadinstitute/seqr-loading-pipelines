FROM docker:dind as BUILD
FROM ghcr.io/astral-sh/uv:python3.11-alpine AS UV_BUILD
FROM hailgenetics/hail:0.2.136-py3.11
COPY --from=BUILD /usr/local/bin/docker /usr/local/bin/docker
COPY --from=UV_BUILD /usr/local/bin/uv /usr/local/bin/uv
LABEL maintainer="Broad TGG"

ARG PIPELINE_RUNNER_APP_VERSION
ENV PIPELINE_RUNNER_APP_VERSION=${PIPELINE_RUNNER_APP_VERSION}

RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH $PATH:/root/google-cloud-sdk/bin

## Copy & Install Application Deps 
WORKDIR /
COPY pyproject.toml uv.lock .
RUN uv sync --no-group dev --locked

# Application Code
WORKDIR /v03_pipeline
COPY v03_pipeline/api api
COPY v03_pipeline/bin bin
COPY v03_pipeline/lib lib
COPY v03_pipeline/ops ops
COPY v03_pipeline/var/liftover var/liftover

# Special paths
COPY v03_pipeline/var/spark_config/spark-defaults.conf /usr/local/lib/python3.10/dist-packages/pyspark/conf/spark-defaults.conf
COPY v03_pipeline/bin/vep /vep


WORKDIR /
EXPOSE 6000 8082
CMD ["uv", "run", "python3", "-m", "v03_pipeline.api"]
