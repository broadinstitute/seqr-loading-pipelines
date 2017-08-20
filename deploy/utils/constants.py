DEPLOYMENT_TARGETS = ["local", "gcloud-dev", "gcloud-prod"]

DEPLOYABLE_COMPONENTS = [
    "cockpit",
    "elasticsearch",
    "kibana",

    "es-client",
    "es-master",
    "es-data",
]

COMPONENT_PORTS = {
    "cockpit":       [9090],
    "elasticsearch": [9200],
    "kibana":        [5601],

    "es-client":     [9200],
    "es-master":     [9020],
    "es-data":     [9020]
}

COMPONENTS_TO_OPEN_IN_BROWSER = set(DEPLOYABLE_COMPONENTS)


