#!/usr/bin/env python

"""
This script creates snapshots of disks bound to PersistentVolumeClaims in the "current" kubernetes cluster.
"""

import argparse
import time
from kubernetes.shell_utils import run

p = argparse.ArgumentParser()
p.add_argument("--zone", help="gcloud zone", default="us-central1-b")
args = p.parse_args()

output = run("kubectl get pvc -o jsonpath='{.items[*].spec.volumeName}'")
disk_names = output.split()

timestamp = time.strftime("%Y%m%d-%H%M%S")
snapshot_names = ["snap-%s--%s" % (timestamp, disk_name) for disk_name in disk_names]

disk_names = " ".join(disk_names)
snapshot_names = ",".join(snapshot_names)
zone = args.zone

run("gcloud compute disks snapshot %(disk_names)s --snapshot-names %(snapshot_names)s --zone=%(zone)s" % locals())