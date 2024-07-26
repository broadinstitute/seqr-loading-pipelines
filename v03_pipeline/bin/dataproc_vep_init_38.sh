#
# VEP init action for dataproc
#
# adapted/copied from
# https://github.com/broadinstitute/gnomad_methods/blob/main/init_scripts/vep105-init.sh
# and gs://hail-common/hailctl/dataproc/0.2.128/vep-GRCh38.sh
#

set -x

export PROJECT="$(gcloud config get-value project)"
export ENVIRONMENT="$(/usr/share/google/get_metadata_value attributes/ENVIRONMENT)"
export VEP_CONFIG_PATH="$(/usr/share/google/get_metadata_value attributes/VEP_CONFIG_PATH)"

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

gcloud storage cp gs://seqr-luigi/releases/$ENVIRONMENT/latest/var/vep_config/vep-GRCh38.json $VEP_CONFIG_PATH

cat >/vep.c <<EOF
#include <unistd.h>
#include <stdio.h>

int
main(int argc, char *const argv[]) {
  if (setuid(geteuid()))
    perror( "setuid" );

  execv("/vep.bash", argv);
  return 0;
}
EOF
gcc -Wall -Werror -O2 /vep.c -o /vep
chmod u+s /vep

gcloud storage cp gs://seqr-luigi/releases/$ENVIRONMENT/latest/bin/download_vep_data.bash /download_vep_data.bash
chmod +x /download_vep_data.bash
./download_vep_data.bash GRCh38 /vep_data

gcloud storage cp gs://seqr-luigi/releases/$ENVIRONMENT/latest/bin/vep /vep.sh
chmod +x /vep.sh

