import glob
import logging
import os
import shutil
import time

from deploy.utils.constants import BASE_DIR
from deploy.utils.servctl_utils import render, retrieve_settings, check_kubernetes_context
from utils.shell_utils import run_shell_command_and_return_output as run

logger = logging.getLogger()


def deploy(deployment_label, component=None, output_dir=None, other_settings={}):
    """
    Args:
        deployment_label (string): one of the DEPLOYMENT_LABELS  (eg. "local", or "gcloud")
        component (string): optionally specifies one of the components from the DEPLOYABLE_COMPONENTS lists (eg. "elasticsearch").
            If this is set to None, all DEPLOYABLE_COMPONENTS will be deployed in sequence.
        output_dir (string): path of directory where to put deployment logs and rendered config files
        other_settings (dict): a dictionary of other key-value pairs for use during deployment
    """

    check_kubernetes_context(deployment_label)

    # parse settings files
    settings = retrieve_settings(deployment_label)
    settings.update(other_settings)

    # configure deployment dir
    timestamp = time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime())
    output_dir = os.path.join(settings["DEPLOYMENT_TEMP_DIR"], "deployments/%(timestamp)s_%(deployment_label)s" % locals())
    settings["DEPLOYMENT_TEMP_DIR"] = output_dir

    # configure logging output
    log_dir = os.path.join(output_dir, "logs")
    if not os.path.isdir(log_dir):
        os.makedirs(log_dir)
    log_file_path = os.path.join(log_dir, "deploy.log")
    sh = logging.StreamHandler(open(log_file_path, "w"))
    sh.setLevel(logging.INFO)
    logger.addHandler(sh)
    logger.info("Starting log file: %(log_file_path)s" % locals())

    # upper-case settings keys
    for key, value in settings.items():
        key = key.upper()
        settings[key] = value
        logger.info("%s = %s" % (key, value))

    # copy settings to output directory
    for file_path in glob.glob("deploy/kubernetes/*.*") + glob.glob("deploy/kubernetes/*/*.*"):
        file_path = file_path.replace('deploy/kubernetes/', '')
        input_base_dir = os.path.join(BASE_DIR, 'deploy/kubernetes')
        output_base_dir = os.path.join(output_dir, 'deploy/kubernetes')
        render(input_base_dir, file_path, settings, output_base_dir)

    for file_path in glob.glob(os.path.join("deploy/kubernetes/*.yaml")):
        try:
            os.makedirs(os.path.join(output_dir, os.path.dirname(file_path)))
        except OSError as e:
            # ignore if the error is that the directory already exists
            # TODO after switch to python3, use exist_ok arg
            if "File exists" not in str(e):
                raise

        shutil.copy(file_path, os.path.join(output_dir, file_path))

    # copy docker directory to output directory
    docker_src_dir = os.path.join(BASE_DIR, "deploy/docker/")
    docker_dest_dir = os.path.join(output_dir, "deploy/docker")
    logger.info("Copying %(docker_src_dir)s to %(docker_dest_dir)s" % locals())
    shutil.copytree(docker_src_dir, docker_dest_dir)

    # deploy
    logger.info("=======================")
    deploy_init(settings)
    logger.info("=======================")
    deploy_elasticsearch(settings)
    logger.info("=======================")
    deploy_kibana(settings)
    #logger.info("=======================")
    #deploy_cockpit(settings)


def _delete_pod(pod_label, settings, async=False):
    if _get_pod_status(pod_label, settings) == "Running":
        run(" ".join([
            "kubectl delete",
                "-f %(DEPLOYMENT_TEMP_DIR)s/deploy/kubernetes/"+pod_label+"/"+pod_label+".%(DEPLOY_TO_PREFIX)s.yaml"
        ]) % settings)

    while _get_pod_status(pod_label, settings) == "Running" and not async:
        time.sleep(5)


def _deploy_pod(pod_label, settings, async=False):
    run(" ".join([
        "kubectl apply",
            "-f %(DEPLOYMENT_TEMP_DIR)s/deploy/kubernetes/"+pod_label+"/"+pod_label+".%(DEPLOY_TO_PREFIX)s.yaml"
    ]) % settings)

    while _get_pod_status(pod_label, settings) != "Running" and not async:
        time.sleep(5)


def deploy_elasticsearch(settings):
    if settings["DELETE_BEFORE_DEPLOY"]:
        _delete_pod("elasticsearch", settings)


    run(" ".join([
        "docker build",
            "--no-cache" if settings["BUILD_DOCKER_IMAGE"] else "",
            "--build-arg ELASTICSEARCH_SERVICE_PORT=%(ELASTICSEARCH_SERVICE_PORT)s",
            "-t %(DOCKER_IMAGE_PREFIX)s/elasticsearch",
            "deploy/docker/elasticsearch/",
    ]) % settings, verbose=True)

    run(" ".join([
        "docker tag",
            "%(DOCKER_IMAGE_PREFIX)s/elasticsearch",
            "%(DOCKER_IMAGE_PREFIX)s/elasticsearch:%(TIMESTAMP)s",
    ]) % settings)


    if settings.get("DEPLOY_TO_PREFIX") == "gcloud":
        run("gcloud docker -- push %(DOCKER_IMAGE_PREFIX)s/elasticsearch:%(TIMESTAMP)s" % settings, verbose=True)

    _deploy_pod("elasticsearch", settings)


def deploy_kibana(settings):
    if settings["DELETE_BEFORE_DEPLOY"]:
        _delete_pod("kibana", settings)

    run(" ".join([
        "docker build",
            "--no-cache" if settings["BUILD_DOCKER_IMAGE"] else "",
            "--build-arg KIBANA_SERVICE_PORT=%(KIBANA_SERVICE_PORT)s",
            "-t %(DOCKER_IMAGE_PREFIX)s/kibana",
            "deploy/docker/kibana/",
        ]) % settings, verbose=True)

    run(" ".join([
        "docker tag ",
        "%(DOCKER_IMAGE_PREFIX)s/kibana",
        "%(DOCKER_IMAGE_PREFIX)s/kibana:%(TIMESTAMP)s",
    ]) % settings)

    if settings.get("DEPLOY_TO_PREFIX") == "gcloud":
        run("gcloud docker -- push %(DOCKER_IMAGE_PREFIX)s/kibana:%(TIMESTAMP)s" % settings, verbose=True)

    _deploy_pod("kibana", settings)


def deploy_cockpit(settings):
    if settings["DELETE_BEFORE_DEPLOY"]:
        _delete_pod("cockpit", settings)

    if settings["DEPLOY_TO_PREFIX"] == "local":
        # disable username/password prompt - https://github.com/cockpit-project/cockpit/pull/6921
        run(" ".join([
            "kubectl create clusterrolebinding anon-cluster-admin-binding",
                "--clusterrole=cluster-admin",
                "--user=system:anonymous",
        ]))

    _deploy_pod("cockpit", settings)

    # print username, password for logging into cockpit
    run("kubectl config view")


def deploy_init(settings):

    if settings["DEPLOY_TO_PREFIX"] == "gcloud":
        run("gcloud config set project %(GCLOUD_PROJECT)s" % settings)


        # create cluster
        try:
            run(" ".join([
                "gcloud container clusters create %(CLUSTER_NAME)s",
                    "--project %(GCLOUD_PROJECT)s",
                    "--zone %(GCLOUD_ZONE)s",
                    "--machine-type %(CLUSTER_MACHINE_TYPE)s",
                    "--num-nodes %(CLUSTER_NUM_NODES)s",
            ]) % settings)
        except RuntimeError as e:
            if "already exists" in str(e):
                logger.info("Cluster already exists")
            else:
                raise e

        run(" ".join([
            "gcloud container clusters get-credentials %(CLUSTER_NAME)s",
                "--project %(GCLOUD_PROJECT)s",
                "--zone %(GCLOUD_ZONE)s",
        ]) % settings)

        # create persistent disk  (200Gb is the minimum recommended by Google)
        try:
            run(" ".join([
                "gcloud compute disks create",
                    "--zone %(GCLOUD_ZONE)s",
                    "--size %(ELASTICSEARCH_DISK_SIZE)s",
                    "%(DEPLOY_TO)s-elasticsearch-disk",
            ]) % settings)
        except RuntimeError as e:
            if "already exists" in str(e):
                logger.info("Disk already exists")
            else:
                raise e
    else:
        run("mkdir -p %(ELASTICSEARCH_DBPATH)s" % settings)

    # initialize the VM
    node_name = _get_node_name()
    if not node_name:
        raise Exception("Unable to retrieve node name. Was the cluster created successfully?")

    # set VM settings required for elasticsearch
    run(" ".join([
        "gcloud compute ssh "+node_name,
            "--zone %(GCLOUD_ZONE)s",
            "--command \"sudo /sbin/sysctl -w vm.max_map_count=4000000\""
    ]) % settings)

    cluster_info, _ = run("kubectl cluster-info")

    # deploy ConfigMap file so that settings key/values can be added as environment variables in each of the pods
    #with open(os.path.join(output_dir, "deploy/kubernetes/all-settings.properties"), "w") as f:
    #    for key, value in settings.items():
    #        f.write("%s=%s\n" % (key, value))

    #run("kubectl delete configmap all-settings")
    #run("kubectl create configmap all-settings --from-file=deploy/kubernetes/all-settings.properties")
    #run("kubectl get configmaps all-settings -o yaml")


def _get_pod_status(pod_label, settings):
    try:
        stdout, _ = run(" ".join([
            "kubectl get pods",
                "-l deployment=%(DEPLOY_TO)s",
                "-l name="+pod_label,
                "-o jsonpath={.items[0].status.phase}",
        ]) % settings)
    except RuntimeError as e:
        if "array index out of bounds: index 0" in str(e):
            return "Not Found"
        else:
            raise e

    logger.info(stdout)
    return stdout


def _get_pod_name(pod_name, settings):
    try:
        stdout, _ = run(" ".join([
            "kubectl get pods",
                "-l deployment=%(DEPLOY_TO)s",
                "-l name="+pod_name,
                "-o jsonpath={.items[0].metadata.name}",
        ]) % settings)
    except RuntimeError as e:
        if "array index out of bounds: index 0" in str(e):
            return None
        else:
            raise e

    return stdout


def _get_node_name():
    try:
        stdout, _ = run(" ".join([
            "kubectl get nodes",
                "-o jsonpath={.items[0].metadata.name}",
        ]) % locals())
    except RuntimeError as e:
        if "array index out of bounds: index 0" in str(e):
            return None
        else:
            raise e

    return stdout
