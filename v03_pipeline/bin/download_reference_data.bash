#!/usr/bin/env bash

set -e

REFERENCE_GENOME=$1

VEP_DATA=vep_data/
SEQR_REFERENCE_DATA=seqr_reference_data/

case ${REFERENCE_GENOME} in
  GRCh38)
    VEP_REFERENCE_DATA_FILES=(
        'gs://seqr-reference-data/vep_data/loftee-beta/GRCh38.tar.gz'
        'gs://seqr-reference-data/vep/GRCh38/AlphaMissense_hg38.tsv.*'
        'gs://seqr-reference-data/vep/GRCh38/Homo_sapiens.GRCh38.dna.primary_assembly.fa.*'
        'gs://seqr-reference-data/vep/GRCh38/homo_sapiens_vep_110_GRCh38.tar.gz'
        'gs://seqr-reference-data/vep/GRCh38/uORF_5UTR_GRCh38_PUBLIC.txt'
    )
    ;;
  GRCh37)
    VEP_REFERENCE_DATA_FILES=(
        'gs://seqr-reference-data/vep_data/loftee-beta/GRCh37.tar.gz'
        'gs://seqr-reference-data/vep/GRCh37/homo_sapiens_vep_110_GRCh37.tar.gz'
        'gs://seqr-reference-data/vep/GRCh37/Homo_sapiens.GRCh37.dna.primary_assembly.fa.*'
    )
    ;;
  *)
    echo "Invalid reference genome '${REFERENCE_GENOME}', should be GRCh37 or GRCh38"
    exit 1
esac


mkdir -p $VEP_DATA/$REFERENCE_GENOME
for vep_reference_data_file in ${VEP_REFERENCE_DATA_FILES[@]}; do
    if  [[ $vep_reference_data_file == *.tar.gz ]]; then
        echo "Downloading and extracting" $vep_reference_data_file;
        gcloud storage cat $vep_reference_data_file | tar -xzf - -C $VEP_DATA/$REFERENCE_GENOME/
    else 
        echo "Downloading" $vep_reference_data_file;
        gcloud storage cp $vep_reference_data_file $VEP_DATA/$REFERENCE_GENOME/
    fi
done;

mkdir -p $SEQR_REFERENCE_DATA
gcloud storage cp -r "gs://seqr-reference-data/v03/$REFERENCE_GENOME/*" $SEQR_REFERENCE_DATA
