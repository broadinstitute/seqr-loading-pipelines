#!/usr/bin/env bash

set -eux

VEP_DATA=${1:~/vep_data}
SEQR_REFERENCE_DATA=${2:~/seqr-reference-data}

for reference_genome in GRCh37 GRCh38; do
    mkdir -p $VEP_DATA/$reference_genome
    mkdir -p $SEQR_REFERENCE_DATA/$reference_genome
    case $reference_genome in
      GRCh37)
        VEP_REFERENCE_DATA_FILES=(
            'gs://seqr-reference-data/vep_data/loftee-beta/GRCh37.tar.gz'
            'gs://seqr-reference-data/vep/GRCh37/homo_sapiens_vep_110_GRCh37.tar.gz'
            'gs://seqr-reference-data/vep/GRCh37/Homo_sapiens.GRCh37.dna.primary_assembly.fa.*'
        )
        ;;
      GRCh38)
        VEP_REFERENCE_DATA_FILES=(
            'gs://seqr-reference-data/vep_data/loftee-beta/GRCh38.tar.gz'
            'gs://seqr-reference-data/vep/GRCh38/AlphaMissense_hg38.tsv.*'
            'gs://seqr-reference-data/vep/GRCh38/Homo_sapiens.GRCh38.dna.primary_assembly.fa.*'
            'gs://seqr-reference-data/vep/GRCh38/homo_sapiens_vep_110_GRCh38.tar.gz'
            'gs://seqr-reference-data/vep/GRCh38/uORF_5UTR_GRCh38_PUBLIC.txt'
        )
        ;;
    esac
    for vep_reference_data_file in ${VEP_REFERENCE_DATA_FILES[@]}; do
        if  [[ $vep_reference_data_file == *.tar.gz ]]; then
            echo "Downloading and extracting" $vep_reference_data_file;
            gcloud storage cat $vep_reference_data_file | tar -xzf - -C $VEP_DATA/$reference_genome/ &
        else 
            echo "Downloading" $vep_reference_data_file;
            gcloud storage cp $vep_reference_data_file $VEP_DATA/$reference_genome/ &
        fi
    done;
    wait
    gcloud storage cp -r "gs://seqr-reference-data/v03/$reference_genome/*" $SEQR_REFERENCE_DATA/$reference_genome/
done;