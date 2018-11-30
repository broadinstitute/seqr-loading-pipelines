#!/bin/bash


# copy VEP and LoFTEE
mkdir -p /vep/homo_sapiens /vep/loftee_data_grch37

curl -Lo hail-elasticsearch-pipelines.zip https://github.com/macarthur-lab/hail-elasticsearch-pipelines/archive/master.zip
unzip -o -d . hail-elasticsearch-pipelines.zip

mv hail-elasticsearch-pipelines-master/loftee /vep
cp hail-elasticsearch-pipelines-master/gcloud_dataproc/vep_init/vep-gcloud-grch37.properties /vep/vep-gcloud.properties
cp hail-elasticsearch-pipelines-master/gcloud_dataproc/vep_init/vep-gcloud-grch37.properties /vep/vep-gcloud-grch37.properties
cp hail-elasticsearch-pipelines-master/gcloud_dataproc/vep_init/1var.vcf /vep/1var.vcf
cp hail-elasticsearch-pipelines-master/gcloud_dataproc/vep_init/run_hail_vep85_GRCh37_vcf.sh /vep/run_hail_vep85_vcf.sh
chmod a+rx /vep/run_hail_vep85_vcf.sh

gsutil -m cp -r gs://hail-common/vep/vep/ensembl-tools-release-85 /vep
gsutil -m cp -r gs://hail-common/vep/vep/GRCh37/loftee_data /vep/loftee_data_grch37
gsutil -m cp -r gs://hail-common/vep/vep/Plugins /vep
gsutil -m cp -r gs://hail-common/vep/vep/homo_sapiens/85_GRCh37 /vep/homo_sapiens/

# create symlink to vep
ln -s /vep/ensembl-tools-release-85/scripts/variant_effect_predictor /vep

chmod -R 777 /vep

# install docker - based on https://docs.docker.com/install/linux/docker-ce/debian/#install-using-the-repository
sudo apt-get update
sudo apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"
sudo apt-get update
sudo apt-get install -y --allow-unauthenticated docker-ce

# run perl 5.20 docker container (with VEP dependencies pre-installed) in place of /usr/local/bin/perl
docker pull weisburd/vep-perl

cat > /perl.c <<EOF
#include <unistd.h>
#include <stdio.h>

int
main(int argc, char *const argv[]) {
  if (setuid(geteuid()))
    perror( "setuid" );

  execv("/perl.sh", argv);
  return 0;
}
EOF

gcc -Wall -Werror -O2 /perl.c -o /usr/local/bin/perl
chmod u+s /usr/local/bin/perl

cat > /perl.sh <<EOF
#!/bin/bash

docker run -i -v /usr/local/sbin:/usr/local/sbin -v /vep:/vep -v $(pwd):/root weisburd/vep-perl \
  "\$@"
EOF
chmod +x /perl.sh

# copy htslib and samtools
gsutil cp gs://hail-common/vep/htslib/* /usr/local/sbin/
gsutil cp gs://hail-common/vep/samtools /usr/local/sbin/
chmod a+rx /usr/local/sbin/tabix
chmod a+rx /usr/local/sbin/bgzip
chmod a+rx /usr/local/sbin/htsfile
chmod a+rx /usr/local/sbin/samtools

# run VEP on the 1-variant VCF to create fasta.index file -- caution do not make fasta.index file writeable afterwards!
/vep/run_hail_vep85_vcf.sh /vep/1var.vcf
