import argparse
import logging
import tempfile

from kubernetes.shell_utils import run

logger = logging.getLogger()



def init_command_line_args():
    p = argparse.ArgumentParser()
    p.add_argument("--k8s-cluster-name", help="Specifies the kubernetes cluster name that hosts elasticsearch.", required=True)
    p.add_argument("--num-persistent-nodes", type=int, help="For use with --num-persistent-nodes. Number of persistent data nodes to create.", default=3)
    p.add_argument("es-disk-snapshots", help="Comma-separated list of disk snapshot names.")

    return p.parse_args()


def main():
    args = init_command_line_args()

    settings = {
        "DEPLOY_TO": args.k8s_cluster_name,
        "CLUSTER_NAME": args.k8s_cluster_name,
        "NAMESPACE": args.k8s_cluster_name,  # kubernetes namespace
        "IMAGE_PULL_POLICY": "Always",
        "ES_NUM_PERSISTENT_NODES": args.num_persistent_nodes,
        "ELASTICSEARCH_DISK_SIZE": "100Gi",
        "ELASTICSEARCH_DISK_SNAPSHOTS": args.es_disk_snapshots.split(",") if args.es_disk_snapshots else None,
    }


    _make_disks(settings, settings["ELASTICSEARCH_DISK_SNAPSHOTS"])


def _make_disks(settings, es_disk_snapshots=None):
    """Create persistent disks from snapshots

    Args:
        es_disk_snapshots (list): optional list of snapshot names
    """

    # create disks from snapshots
    created_disks = []
    if es_disk_snapshots:
        for i, snapshot_name in enumerate(es_disk_snapshots):
            disk_name = "es-data-%s--%d" % (settings["CLUSTER_NAME"], i)   # time.strftime("%y%m%d-%H%M%S")  - make the timestamp year-month-day so a bunch of disks don't get created accidentally

            run(" ".join([
                "gcloud compute disks create " + disk_name,
                "--type pd-ssd",
                "--source-snapshot " + snapshot_name,
                ]) % settings, errors_to_ignore=["lready exists"])

            disk_size =  settings["ELASTICSEARCH_DISK_SIZE"] # TODO GET SNAPSHOT DISK SIZE from gcloud compute disks describe ...

            created_disks.append((disk_name, disk_size))
    else:
        for i in range(settings["ES_NUM_PERSISTENT_NODES"]):
            disk_name = "es-data-%s--%d" % (settings["CLUSTER_NAME"], i)

            run(" ".join([
                "gcloud compute disks create " + disk_name,
                "--type pd-ssd",
                "--size %(ELASTICSEARCH_DISK_SIZE)s",
                ]) % settings, errors_to_ignore=["lready exists"])

            created_disks.append((disk_name, settings["ELASTICSEARCH_DISK_SIZE"]))


    # create PersistentVolume objects for disk
    namespace = settings["NAMESPACE"]
    for i, (existing_disk_name, elasticsearch_disk_size) in enumerate(created_disks):

        with tempfile.NamedTemporaryFile("w") as f:
            f.write("""apiVersion: v1
kind: PersistentVolume
metadata:
  name: %(existing_disk_name)s
  namespace: %(namespace)s
spec:
  capacity:
    storage: %(elasticsearch_disk_size)s
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ssd-storage-class
  gcePersistentDisk:
    fsType: ext4
    pdName: %(existing_disk_name)s
""" % locals())

            f.flush()
            file_path = f.name
            run("kubectl create -f %(file_path)s" % locals(), print_command=True, errors_to_ignore=["already exists"])


if __name__ == "__main__":
    main()
