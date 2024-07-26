#!/usr/bin/env bash

set -eux

REFERENCE_GENOME=${1:-GRCh38}
SEQR_REFERENCE_DATA=${2:~/seqr-reference-data}

case $REFERENCE_GENOME in
  GRCh38)
    ;;
  GRCh37)
    ;;
   *)
    echo "Invalid reference genome $REFERENCE_GENOME, should be GRCh37 or GRCh38"
    exit 1
esac

mkdir -p $SEQR_REFERENCE_DATA/$reference_genome;
gcloud storage cp -r "gs://seqr-reference-data/v03/$reference_genome/*" $SEQR_REFERENCE_DATA/$reference_genome/
