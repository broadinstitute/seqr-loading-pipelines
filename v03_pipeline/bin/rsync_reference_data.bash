#!/usr/bin/env bash

set -eux

REFERENCE_GENOME=$1
REFERENCE_DATASETS_DIR=${REFERENCE_DATASETS_DIR:-/var/seqr/seqr-reference-data}


case $REFERENCE_GENOME in
  GRCh38)
    ;;
  GRCh37)
    ;;
   *)
    echo "Invalid reference genome $REFERENCE_GENOME, should be GRCh37 or GRCh38"
    exit 1
esac

mkdir -p $REFERENCE_DATASETS_DIR/$REFERENCE_GENOME;

if [ -f "$REFERENCE_DATASETS_DIR"/"$REFERENCE_GENOME"/_SUCCESS ]; then
   echo "Skipping rsync because already successful"
   exit 0;
fi

gsutil -m rsync -rd "gs://seqr-reference-data/v03/$REFERENCE_GENOME" $REFERENCE_DATASETS_DIR/$REFERENCE_GENOME
touch "$REFERENCE_DATASETS_DIR"/"$REFERENCE_GENOME"/_SUCCESS
