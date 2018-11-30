FROM perl:5.20

MAINTAINER Ben Weisburd

RUN apt-get update \
    && apt-get install -y cpanminus \
    && cpanm install --notest DBI \
    && cpanm install --notest JSON \
    && cpanm install --notest DBD::SQLite

ENTRYPOINT [ "perl" ]
