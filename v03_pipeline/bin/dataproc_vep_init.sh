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
export REFERENCE_GENOME=GRCh38
export VEP_DOCKER_IMAGE=gcr.io/seqr-project/vep-docker-image:GRCh38

mkdir -p /vep_data/$REFERENCE_GENOME

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
gcloud storage cp --billing-project $PROJECT gs://seqr-reference-data/vep/GRCh38/vep-${REFERENCE_GENOME}.json $VEP_CONFIG_PATH
docker pull ${VEP_DOCKER_IMAGE}

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


gcloud storage cp --billing-project $PROJECT 'gs://seqr-reference-data/vep/GRCh38/AlphaMissense_hg38.tsv.*' /vep_data/$REFERENCE_GENOME/ &
chmod +x /vep.sh

