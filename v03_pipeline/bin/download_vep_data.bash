#!/usr/bin/env bash

set -eux

REFERENCE_GENOME=${1:-GRCh38}
VEP_DATA=${2:-/vep_data}

case $REFERENCE_GENOME in
  GRCh38)
    VEP_REFERENCE_DATA_FILES=(
        'gs://seqr-reference-data/vep_data/loftee-beta/GRCh38.tar.gz'

        # Raw data files copied from the bucket (https://console.cloud.google.com/storage/browser/dm_alphamissense;tab=objects?prefix=&forceOnObjectsSortingFiltering=false)
        # tabix -s 1 -b 2 -e 2 -f -S 1 AlphaMissense_hg38.tsv.gz
        'gs://seqr-reference-data/vep/GRCh38/AlphaMissense_hg38.tsv.*'
        
        # Generated with:
        # curl -O ftp://ftp.ensembl.org/pub/release-110/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz > Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
        # gzip -d Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
        # bgzip Homo_sapiens.GRCh38.dna.primary_assembly.fa
        # samtools faidx Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
        'gs://seqr-reference-data/vep/GRCh38/Homo_sapiens.GRCh38.dna.primary_assembly.fa.*'

        # Copied from ftp://ftp.ensembl.org/pub/release-110/variation/indexed_vep_cache/homo_sapiens_vep_110_GRCh38.tar.gz
        'gs://seqr-reference-data/vep/GRCh38/homo_sapiens_vep_110_GRCh38.tar.gz'

        # Copied from the UTRAnnotator repo (https://github.com/ImperialCardioGenetics/UTRannotator/tree/master)
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
    echo "Invalid reference genome $REFERENCE_GENOME, should be GRCh37 or GRCh38"
    exit 1
esac

if [ -f $VEP_DATA/$REFERENCE_GENOME/_SUCCESS ]; then
   echo "Skipping download because already successful"
   exit 0;
fi

mkdir -p $VEP_DATA/$REFERENCE_GENOME;
for vep_reference_data_file in ${VEP_REFERENCE_DATA_FILES[@]}; do
    if  [[ $vep_reference_data_file == *.tar.gz ]]; then
        echo "Downloading and extracting" $vep_reference_data_file;
        gcloud storage cat $vep_reference_data_file | tar -xzf - -C $VEP_DATA/$REFERENCE_GENOME/ &
    else 
        echo "Downloading" $vep_reference_data_file;
        gcloud storage cp $vep_reference_data_file $VEP_DATA/$REFERENCE_GENOME/ &
    fi
done;
wait
touch $VEP_DATA/$REFERENCE_GENOME/_SUCCESS
