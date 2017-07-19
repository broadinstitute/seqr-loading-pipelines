#!/usr/bin/env bash

gcloud dataproc jobs kill $1
#`gcloud dataproc jobs list --state-filter=active |& cut -f 1 |& grep -v Listed`
