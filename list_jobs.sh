#!/usr/bin/env bash

CLUSTER=$1
gcloud dataproc jobs list --cluster=$1 | head