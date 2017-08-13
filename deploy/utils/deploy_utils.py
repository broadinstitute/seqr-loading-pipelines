import glob
import logging
import os
import time


from deploy.utils.kubectl_utils import get_pod_status, get_pod_name, \
    run_in_pod, get_node_name, POD_READY_STATUS, POD_RUNNING_STATUS
from utils.shell_utils import run
from deploy.utils.servctl_utils import render, check_kubernetes_context, retrieve_settings

logger = logging.getLogger(__name__)


def deploy(deployment_target, components=None, output_dir=None, other_settings={}):
    """Deploy all seqr components to a kubernetes cluster.
    Args:
        deployment_target (string): one of the DEPLOYMENT_TARGETs  (eg. "local", or "gcloud")
        components (list): If set to component names from constants.DEPLOYABLE_COMPONENTS,
            (eg. "kibana", "elasticsearch"), only these components will be deployed.  If not set,
            all DEPLOYABLE_COMPONENTS will be deployed in sequence.
        output_dir (string): path of directory where to put deployment logs and rendered config files
        other_settings (dict): a dictionary of other key-value pairs for use during deployment
    """

    check_kubernetes_context(deployment_target)

    # parse settings files
    settings = retrieve_settings(deployment_target)
    settings.update(other_settings)

    # configure deployment dir
    timestamp = time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime())
    output_dir = os.path.join(settings["DEPLOYMENT_TEMP_DIR"], "deployments/%(timestamp)s_%(deployment_target)s" % locals())
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

    # render Jinja templates and put results in output directory
    for file_path in glob.glob("deploy/kubernetes/*.yaml") + glob.glob("deploy/kubernetes/*/*.yaml"):
        file_path = file_path.replace('deploy/kubernetes/', '')

        input_base_dir = os.path.join(other_settings["BASE_DIR"], 'deploy/kubernetes')
        output_base_dir = os.path.join(output_dir, 'deploy/kubernetes')

        render(input_base_dir, file_path, settings, output_base_dir)

    # deploy
    deploy_init(settings)

    if not components or "cockpit" in components:
        if components or settings["DEPLOY_TO"] != "local":
            deploy_cockpit(settings)
    #if not components or "elasticsearch" in components:
    #    deploy_elasticsearch(settings)
    if not components or "elasticsearch" in components:
        if settings["DEPLOY_TO"] == "local":
            deploy_elasticsearch(settings)
        else:
            deploy_elasticsearch_sharded(settings)

    if not components or "kibana" in components:
        deploy_kibana(settings)


def deploy_elasticsearch_sharded(settings):
    print_separator("elasticsearch-sharded")

    elasticsearch_config_files = [
        "%(DEPLOYMENT_TEMP_DIR)s/deploy/kubernetes/elasticsearch-sharded/es-discovery-svc.yaml",
        "%(DEPLOYMENT_TEMP_DIR)s/deploy/kubernetes/elasticsearch-sharded/es-svc.yaml",
        "%(DEPLOYMENT_TEMP_DIR)s/deploy/kubernetes/elasticsearch-sharded/es-master.yaml",
        "%(DEPLOYMENT_TEMP_DIR)s/deploy/kubernetes/elasticsearch-sharded/es-client.yaml",
        "%(DEPLOYMENT_TEMP_DIR)s/deploy/kubernetes/elasticsearch-sharded/es-data-svc.yaml",
        "%(DEPLOYMENT_TEMP_DIR)s/deploy/kubernetes/elasticsearch-sharded/es-data-stateful.yaml",
    ]

    if settings["DELETE_BEFORE_DEPLOY"]:
        for config_file in elasticsearch_config_files:
            run("kubectl delete -f " + config_file % settings, errors_to_ignore=["not found"])

    for config_file in elasticsearch_config_files:
        run("kubectl create -f " + config_file % settings, errors_to_ignore=["already exists"])
        if config_file.endswith("es-master.yaml"):
            _wait_until_pod_is_running("es-master", deployment_target=settings["DEPLOY_TO"])
        elif config_file.endswith("es-data-stateful.yaml"):
            _wait_until_pod_is_running("elasticsearch", deployment_target=settings["DEPLOY_TO"])

    run("kubectl describe svc elasticsearch")

def _delete_pod(component_label, settings, async=False, custom_yaml_filename=None):
    yaml_filename = custom_yaml_filename or (component_label+".%(DEPLOY_TO_PREFIX)s.yaml")

    deployment_target = settings["DEPLOY_TO"]
    if get_pod_status(component_label, deployment_target) == "Running":
        run(" ".join([
            "kubectl delete",
            "-f %(DEPLOYMENT_TEMP_DIR)s/deploy/kubernetes/"+component_label+"/"+yaml_filename,
        ]) % settings, errors_to_ignore=["not found"])

    logger.info("waiting for \"%s\" to exit Running status" % component_label)
    while get_pod_status(component_label, deployment_target) == "Running" and not async:
        time.sleep(5)


def _wait_until_pod_is_running(component_label, deployment_target):
    logger.info("waiting for \"%s\" to enter Running state" % component_label)
    while get_pod_status(component_label, deployment_target, status_type=POD_RUNNING_STATUS) != "Running":
        time.sleep(5)


def _wait_until_pod_is_ready(component_label, deployment_target):
    logger.info("waiting for \"%s\" to complete initialization" % component_label)
    while get_pod_status(component_label, deployment_target, status_type=POD_READY_STATUS) != "true":
        time.sleep(5)


def _deploy_pod(component_label, settings, wait_until_pod_is_running=True, wait_until_pod_is_ready=False):
    run(" ".join([
        "kubectl apply",
        "-f %(DEPLOYMENT_TEMP_DIR)s/deploy/kubernetes/"+component_label+"/"+component_label+".%(DEPLOY_TO_PREFIX)s.yaml"
    ]) % settings)

    if wait_until_pod_is_running:
        _wait_until_pod_is_running(component_label, deployment_target=settings["DEPLOY_TO"])

    if wait_until_pod_is_ready:
        _wait_until_pod_is_ready(component_label, deployment_target=settings["DEPLOY_TO"])


def _docker_build(component_label, settings, custom_build_args=[]):
    settings = dict(settings)  # make a copy before modifying
    settings["COMPONENT_LABEL"] = component_label

    run(" ".join([
            "docker build"
        ] + custom_build_args + [
            "--no-cache" if settings["BUILD_DOCKER_IMAGE"] else "",
            "-t %(DOCKER_IMAGE_PREFIX)s/%(COMPONENT_LABEL)s",
            "deploy/docker/%(COMPONENT_LABEL)s/",
    ]) % settings, verbose=True)

    run(" ".join([
        "docker tag",
            "%(DOCKER_IMAGE_PREFIX)s/%(COMPONENT_LABEL)s",
            "%(DOCKER_IMAGE_PREFIX)s/%(COMPONENT_LABEL)s:%(TIMESTAMP)s",
    ]) % settings)


    if settings.get("DEPLOY_TO_PREFIX") == "gcloud":
        run("gcloud docker -- push %(DOCKER_IMAGE_PREFIX)s/%(COMPONENT_LABEL)s:%(TIMESTAMP)s" % settings, verbose=True)


def deploy_elasticsearch(settings):
    print_separator("elasticsearch")

    if settings["DELETE_BEFORE_DEPLOY"]:
        _delete_pod("elasticsearch", settings)

    _docker_build(
        "elasticsearch",
        settings,
        ["--build-arg ELASTICSEARCH_SERVICE_PORT=%s" % settings["ELASTICSEARCH_SERVICE_PORT"]],
    )

    _deploy_pod("elasticsearch", settings, wait_until_pod_is_ready=True)

def deploy_kibana(settings):
    print_separator("kibana")

    if settings["DELETE_BEFORE_DEPLOY"]:
        _delete_pod("kibana", settings)

    _docker_build(
        "kibana",
        settings,
        ["--build-arg KIBANA_SERVICE_PORT=%s" % settings["KIBANA_SERVICE_PORT"]],
    )

    _deploy_pod("kibana", settings, wait_until_pod_is_ready=True)


def deploy_cockpit(settings):
    print_separator("cockpit")

    if settings["DELETE_BEFORE_DEPLOY"]:
        _delete_pod("cockpit", settings, custom_yaml_filename="cockpit.yaml")
        #"kubectl delete -f %(DEPLOYMENT_TEMP_DIR)s/deploy/kubernetes/cockpit/cockpit.yaml" % settings,


    # disable username/password prompt - https://github.com/cockpit-project/cockpit/pull/6921
    run(" ".join([
        "kubectl create clusterrolebinding anon-cluster-admin-binding",
            "--clusterrole=cluster-admin",
            "--user=system:anonymous",
    ]), errors_to_ignore=["already exists"])

    run("kubectl apply -f %(DEPLOYMENT_TEMP_DIR)s/deploy/kubernetes/cockpit/cockpit.yaml" % settings)

    # print username, password for logging into cockpit
    run("kubectl config view")


def deploy_init(settings):
    """Provisions a GKE cluster, persistant disks, and any other prerequisites for deployment."""

    print_separator("init")

    if settings["DEPLOY_TO_PREFIX"] == "gcloud":
        run("gcloud config set project %(GCLOUD_PROJECT)s" % settings)

        # create private network for cluster and dataproc
        # based on: https://medium.com/@DazWilkin/gkes-cluster-ipv4-cidr-flag-69d25884a558
        run(" ".join([
            #"gcloud compute networks create seqr-project-custom-vpc --project=%(GCLOUD_PROJECT)s --mode=custom"
            "gcloud compute networks create %(GCLOUD_PROJECT)s-auto-vpc",
                "--project=%(GCLOUD_PROJECT)s",
                "--mode=auto"
        ]) % settings, errors_to_ignore=["already exists"])

        # add recommended firewall rules to enable ssh, etc.
        run(" ".join([
            "gcloud compute firewall-rules create custom-vpc-allow-tcp-udp-icmp",
            "--project %(GCLOUD_PROJECT)s",
            "--network %(GCLOUD_PROJECT)s-auto-vpc",
            "--allow tcp,udp,icmp",
            "--source-ranges 10.0.0.0/8",
        ]) % settings, errors_to_ignore=["already exists"])

        run(" ".join([
            "gcloud compute firewall-rules create custom-vpc-allow-ports",
                "--project %(GCLOUD_PROJECT)s",
                "--network %(GCLOUD_PROJECT)s-auto-vpc",
                "--allow tcp:22,tcp:3389,icmp",
                "--source-ranges 10.0.0.0/8",
        ]) % settings, errors_to_ignore=["already exists"])


        # create cluster
        run(" ".join([
            "gcloud container clusters create %(CLUSTER_NAME)s",
            "--project %(GCLOUD_PROJECT)s",
            "--zone %(GCLOUD_ZONE)s",
            "--network %(GCLOUD_PROJECT)s-auto-vpc",
            "--machine-type %(CLUSTER_MACHINE_TYPE)s",
            "--num-nodes %(CLUSTER_NUM_NODES)s",
        ]) % settings, verbose=False, errors_to_ignore=["already exists"])

        run(" ".join([
            "gcloud container clusters get-credentials %(CLUSTER_NAME)s",
            "--project %(GCLOUD_PROJECT)s",
            "--zone %(GCLOUD_ZONE)s",
        ]) % settings)

        #run(" ".join([
        #    "gcloud container clusters resize %(CLUSTER_NAME)s --size %(CLUSTER_NUM_NODES)s" % settings,
        #]), is_interactive=True)

    #node_name = get_node_name()
    #if not node_name:
    #    raise Exception("Unable to retrieve node name. Was the cluster created successfully?")

    # deploy ConfigMap file so that settings key/values can be added as environment variables in each of the pods
    #with open(os.path.join(output_dir, "deploy/kubernetes/all-settings.properties"), "w") as f:
    #    for key, value in settings.items():
    #        f.write("%s=%s\n" % (key, value))

    #run("kubectl delete configmap all-settings")
    #run("kubectl create configmap all-settings --from-file=deploy/kubernetes/all-settings.properties")
    #run("kubectl get configmaps all-settings -o yaml")

    run("kubectl cluster-info", verbose=True)


def print_separator(label):
    message = "       DEPLOY %s       " % str(label)
    logger.info("=" * len(message))
    logger.info(message)
    logger.info("=" * len(message) + "\n")

