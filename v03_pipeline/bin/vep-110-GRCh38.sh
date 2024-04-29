#
# VEP init action for dataproc
#
# adapted/copied from
# https://github.com/broadinstitute/gnomad_methods/blob/main/init_scripts/vep105-init.sh
# and gs://hail-common/hailctl/dataproc/0.2.128/vep-GRCh38.sh
#

set -x

export PROJECT="$(gcloud config get-value project)"
export VEP_CONFIG_PATH="$(/usr/share/google/get_metadata_value attributes/VEP_CONFIG_PATH)"
export VEP_REPLICATE="$(/usr/share/google/get_metadata_value attributes/VEP_REPLICATE)"
export ASSEMBLY=GRCh38
export VEP_DOCKER_IMAGE=gcr.io/seqr-project/vep-docker-image

mkdir -p /vep_data

# Install docker
apt-get update
apt-get -y install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg2 \
    software-properties-common \
    tabix
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"
apt-get update
apt-get install -y --allow-unauthenticated docker-ce

# https://github.com/hail-is/hail/issues/12936
sleep 60
sudo service docker restart

# Copied from the repo at v03_pipeline/var/vep_config
gcloud storage cp --billing-project $PROJECT gs://seqr-reference-data/vep/110/vep-${ASSEMBLY}.json $VEP_CONFIG_PATH

# Copied from the UTRAnnotator repo (https://github.com/ImperialCardioGenetics/UTRannotator/tree/master)
gcloud storage cp --billing-project $PROJECT gs://seqr-reference-data/vep/110/uORF_5UTR_${ASSEMBLY}_PUBLIC.txt /vep_data/ &

# Raw data files copied from the bucket (https://console.cloud.google.com/storage/browser/dm_alphamissense;tab=objects?prefix=&forceOnObjectsSortingFiltering=false)
# Some investigation led us to want to combine the canonical and non-canonical transcript tsvs (run inside the VEP docker container):
# cat AlphaMissense_hg38.tsv.gz | gunzip | grep -v '#' | awk 'BEGIN { OFS = "\t" };{$6=""; print $0}' > AlphaMissense_combined_hg38.tsv
# cat AlphaMissense_isoforms_hg38.tsv.gz | gunzip | grep -v '#' >> AlphaMissense_combined_hg38.tsv
# cat AlphaMissense_combined_hg38.tsv | sort --parallel=12 --buffer-size=20G -k1,1 -k2,2n > AlphaMissense_combined_sorted_hg38.tsv
# cat AlphaMissense_combined_sorted_hg38.tsv | sed '1i #CHROM\tPOS\tREF\tALT\tgenome\ttranscript_id\tprotein_variant\tam_pathogenicity\tam_class' > AlphaMissense_hg38.tsv
# bgzip AlphaMissense_hg38.tsv
# tabix -s 1 -b 2 -e 2 -f -S 1 AlphaMissense_hg38.tsv.gz
gcloud storage cp --billing-project $PROJECT 'gs://seqr-reference-data/vep/110/AlphaMissense_hg38.tsv.*' /vep_data/ &

gcloud storage cat --billing-project $PROJECT gs://seqr-reference-data/vep_data/loftee-beta/${ASSEMBLY}.tar | tar -xf - -C /vep_data/ &

# Copied from ftp://ftp.ensembl.org/pub/release-110/variation/indexed_vep_cache/homo_sapiens_merged_vep_110_${ASSEMBLY}.tar.gz
gcloud storage cat --billing-project $PROJECT gs://seqr-reference-data/vep/110/homo_sapiens_vep_110_${ASSEMBLY}.tar.gz | tar -xzf - -C /vep_data/ &

# Generated with:
# curl -O ftp://ftp.ensembl.org/pub/release-110/fasta/homo_sapiens/dna/Homo_sapiens.${ASSEMBLY}.dna.primary_assembly.fa.gz > Homo_sapiens.${ASSEMBLY}.dna.primary_assembly.fa.gz
# gzip -d Homo_sapiens.${ASSEMBLY}.dna.primary_assembly.fa.gz
# bgzip Homo_sapiens.${ASSEMBLY}.dna.primary_assembly.fa
# samtools faidx Homo_sapiens.${ASSEMBLY}.dna.primary_assembly.fa.gz
gcloud storage cp --billing-project $PROJECT 'gs://seqr-reference-data/vep/110/Homo_sapiens.${ASSEMBLY}.dna.primary_assembly.fa.*' /vep_data/ &
docker pull ${VEP_DOCKER_IMAGE} &
wait

cat >/vep.c <<EOF
#include <unistd.h>
#include <stdio.h>

int
main(int argc, char *const argv[]) {
  if (setuid(geteuid()))
    perror( "setuid" );

  execv("/vep.sh", argv);
  return 0;
}
EOF
gcc -Wall -Werror -O2 /vep.c -o /vep
chmod u+s /vep

cat >/vep.sh <<EOF
#!/bin/bash

docker run -i -v /vep_data/:/opt/vep/.vep/:ro ${VEP_DOCKER_IMAGE} \
  /opt/vep/src/ensembl-vep/vep "\$@"
EOF
chmod +x /vep.sh
