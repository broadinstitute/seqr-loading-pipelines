import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

DEPLOYMENT_LABELS = ["local", "gcloud-dev", 'gcloud-prod']
DEPLOYABLE_COMPONENTS = [
    'cockpit',
    'elasticsearch',
    'kibana',
]

PORTS = {
    'cockpit':   [9090],
    'elasticsearch': [30001],
    'kibana':        [30002],
}


DEPLOYMENT_SCRIPTS = [
    'kubernetes/scripts/deploy_begin.sh',
    'kubernetes/scripts/deploy_cockpit.sh',
    'kubernetes/scripts/deploy_elasticsearch.sh',
    'kubernetes/scripts/deploy_kibana.sh',
]


COMPONENTS_TO_OPEN_IN_BROWSER = [
    'cockpit',
    'elasticsearch',
    'kibana',
]
