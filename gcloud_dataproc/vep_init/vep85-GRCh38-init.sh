#!/bin/bash


# Copy VEP and LoFTEE
mkdir -p /vep/homo_sapiens /vep/loftee_data_grch38

curl -Lo hail-elasticsearch-pipelines.zip https://github.com/macarthur-lab/hail-elasticsearch-pipelines/archive/master.zip
unzip -o -d . hail-elasticsearch-pipelines.zip

mv hail-elasticsearch-pipelines-master/loftee /vep
cp hail-elasticsearch-pipelines-master/gcloud_dataproc/cluster_init/vep-gcloud-grch38.properties /vep/vep-gcloud.properties
cp hail-elasticsearch-pipelines-master/gcloud_dataproc/cluster_init/vep-gcloud-grch38.properties /vep/vep-gcloud-grch38.properties
cp hail-elasticsearch-pipelines-master/gcloud_dataproc/cluster_init/1var.vcf /vep/1var.vcf
cp hail-elasticsearch-pipelines-master/gcloud_dataproc/cluster_init/run_hail_vep85_GRCh38_vcf.sh /vep/run_hail_vep85_vcf.sh
chmod a+rx /vep/run_hail_vep85_vcf.sh

gsutil -m cp -r gs://hail-common/vep/vep/ensembl-tools-release-85 /vep
gsutil -m cp -r gs://hail-common/vep/vep/GRCh38/loftee_data /vep/loftee_data_grch38
gsutil -m cp -r gs://hail-common/vep/vep/Plugins /vep
gsutil -m cp -r gs://hail-common/vep/vep/homo_sapiens/85_GRCh38 /vep/homo_sapiens/

#Create symlink to vep
ln -s /vep/ensembl-tools-release-85/scripts/variant_effect_predictor /vep

#Give perms
chmod -R 777 /vep

sudo ln -s /usr/bin/perl /usr/local/bin/perl

# Copy perl JSON module
gsutil -m cp -r gs://hail-common/vep/perl-JSON/* /usr/share/perl/5.20/

#Copy perl DBD::SQLite module
gsutil -m cp -r gs://hail-common/vep/perl-SQLITE/* /usr/share/perl/5.20/

sudo apt-get install -y cpanminus
sudo cpanm install DBI

# Copy htslib and samtools
gsutil cp gs://hail-common/vep/htslib/* /usr/bin/
gsutil cp gs://hail-common/vep/samtools /usr/bin/
chmod a+rx  /usr/bin/tabix
chmod a+rx  /usr/bin/bgzip
chmod a+rx  /usr/bin/htsfile
chmod a+rx  /usr/bin/samtools

#Run VEP on the 1-variant VCF to create fasta.index file -- caution do not make fasta.index file writeable afterwards!
/vep/run_hail_vep85_vcf.sh /vep/1var.vcf
