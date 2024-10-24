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

case $REFERENCE_DATASETS_DIR in
  "gs://seqr-reference-data")
    echo "Cannot rsync to the authoritative source"
    exit 1
    ;;
    *)
    ;;
esac

if ! [[ "$REFERENCE_DATASETS_DIR" =~ gs://* ]]; then
  mkdir -p $REFERENCE_DATASETS_DIR/$REFERENCE_GENOME;
  if [ -f "$REFERENCE_DATASETS_DIR"/"$REFERENCE_GENOME"/_SUCCESS ]; then
     echo "Skipping rsync because already successful"
     exit 0;
  fi
else
  result=$(gsutil -q stat "$REFERENCE_DATASETS_DIR"/"$REFERENCE_GENOME"/_SUCCESS || echo 1)
  if [[ $result != 1 ]]; then
    echo "Skipping rsync because already successful"
    exit 0;
  fi
fi

gsutil -m rsync -rd "gs://seqr-reference-data/v03/$REFERENCE_GENOME" $REFERENCE_DATASETS_DIR/$REFERENCE_GENOME
if ! [[ $REFERENCE_DATASETS_DIR =~ gs://* ]]; then
  touch "$REFERENCE_DATASETS_DIR"/"$REFERENCE_GENOME"/_SUCCESS
else 
  touch _SUCCESS
  gsutil cp _SUCCESS "$REFERENCE_DATASETS_DIR"/"$REFERENCE_GENOME"/_SUCCESS
  rm -rf _SUCCESS
fi