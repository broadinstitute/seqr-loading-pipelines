#!/usr/bin/env bash

set -eux

REFERENCE_GENOME=$1
STRVCTVRE_REFERENCE_DATASETS_DIR=${STRVCTVRE_REFERENCE_DATASETS_DIR:-/var/seqr/strvctvre-reference-data}

case $REFERENCE_GENOME in
  GRCh38)
    STRVCTVRE_REFERENCE_DATA_FILES=(
        'gs://seqr-reference-data/strvctvre_data/hg38.phyloP100way.bw'
    )
    ;;
   *)
    echo "Invalid reference genome $REFERENCE_GENOME, should be GRCh38"
    exit 1
esac

if [ -f "$STRVCTVRE_REFERENCE_DATASETS_DIR"/"$REFERENCE_GENOME"/_SUCCESS ]; then
   echo "Skipping download because already successful"
   exit 0;
fi

mkdir -p "$STRVCTVRE_REFERENCE_DATASETS_DIR"/"$REFERENCE_GENOME";
rm -rf "${STRVCTVRE_REFERENCE_DATASETS_DIR:?}"/"${REFERENCE_GENOME:?}"/*;

for reference_data_file in "${STRVCTVRE_REFERENCE_DATA_FILES[@]}"; do
    echo "Downloading" "$reference_data_file";
    gsutil cp "$reference_data_file" "$STRVCTVRE_REFERENCE_DATASETS_DIR"/"$REFERENCE_GENOME"/ &
done;
wait
touch "$STRVCTVRE_REFERENCE_DATASETS_DIR"/"$REFERENCE_GENOME"/_SUCCESS
