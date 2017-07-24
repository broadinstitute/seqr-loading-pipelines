#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
source ${SCRIPT_DIR}/init_env.sh

echo Current Directory: `pwd`

set -x

if [ "$DEPLOY_TO_PREFIX" = 'gcloud' ]; then
    gcloud config set project $GCLOUD_PROJECT

    # create cluster
    gcloud container clusters create $CLUSTER_NAME \
    --project $GCLOUD_PROJECT \
    --zone $GCLOUD_ZONE \
    --machine-type $CLUSTER_MACHINE_TYPE \
    --num-nodes $CLUSTER_NUM_NODES

    gcloud container clusters get-credentials $CLUSTER_NAME \
    --project $GCLOUD_PROJECT \
    --zone $GCLOUD_ZONE

    # create persistent disk  (200Gb is the minimum recommended by Google)
    gcloud compute disks create --size ${ELASTICSEARCH_DISK_SIZE} ${DEPLOY_TO}-elasticsearch-disk --zone $GCLOUD_ZONE

else
    mkdir -p ${ELASTICSEARCH_DBPATH}
fi

# initialize the VM
NODE_NAME="$(get_node_name $CLUSTER_NAME)"

# set VM settings required for elasticsearch
gcloud compute ssh $NODE_NAME --command "sudo /sbin/sysctl -w vm.max_map_count=4000000"


echo Cluster Info:
kubectl cluster-info

# deploy config map
kubectl delete configmap all-settings
kubectl create configmap all-settings --from-file=kubernetes/settings/all-settings.properties
kubectl get configmaps all-settings -o yaml
