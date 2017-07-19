#!/usr/bin/env bash

# resize cluster
if [ -z $1 ]; then
    CLUSTER=$(gcloud dataproc clusters list | cut -f 1 -d \ | grep -v NAME | grep dataproc-cluster | head -n 1)
else
    CLUSTER=$1
fi

if [ -z $CLUSTER ]; then
    echo ERROR: cluster doesn\'t exist
    exit 0
fi

if [ -z $2 ] || [ -z $3 ]; then
    NUM_WORKERS=5
    NUM_PREEMPTIBLE_WORKERS=20
else
    NUM_WORKERS=$2
    NUM_PREEMPTIBLE_WORKERS=$3
fi

gcloud dataproc clusters update --num-workers $NUM_WORKERS --num-preemptible-workers $NUM_PREEMPTIBLE_WORKERS $CLUSTER
