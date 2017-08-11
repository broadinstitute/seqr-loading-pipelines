import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

DEPLOYMENT_LABELS = ["local", "gcloud-dev", 'gcloud-prod']

COMPONENT_PORTS = {
    'cockpit':   [9090],
    'elasticsearch': [30001],
    'kibana':        [30002],
}

COMPONENTS_TO_OPEN_IN_BROWSER = [
    'cockpit',
    'elasticsearch',
    'kibana',
]

DEPLOYABLE_COMPONENTS = list(COMPONENT_PORTS.keys())
