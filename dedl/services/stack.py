import getpass, gettext
from dask_gateway import Gateway, BasicAuth
from distributed import Client


class DaskCluster:
    gateway_registry = {
        "central-site": "https://dedl-centralsite.eodc.eu/dask",
        "bridge": "https://dedl-bridge.eodc.eu/dask",
    }

    cluster_scale_limits = {
        "central-site": {"min": 2, "max": 20},
        "bridge": {"min": 2, "max": 10},
    }

    def __init__(self, name):
        self.name = name

    def login(self, username) -> BasicAuth:
        return BasicAuth(username, getpass.getpass())

    def get_gateways(self) -> None:
        for site in self.gateway_registry:
            print(f"{site}: {self.gateway_registry[site]}")

    def create_cluster(self, authobj: BasicAuth) -> None:
        self.gateway = {}
        self.cluster = {}
        self.client = {}

        for site in self.gateway_registry:
            # connect to gateway
            self.gateway[site] = Gateway(
                address=self.gateway_registry[site],
                auth=authobj,
            )
            # get new cluster object
            self.cluster[site] = self.gateway[site].new_cluster(
                worker_cores=1,
                worker_memory=1,
                image="registry.eodc.eu/eodc/dedl_demo:1.0",
            )
            self.cluster[site].adapt(
                minimum=self.cluster_scale_limits[site]["min"],
                maximum=self.cluster_scale_limits[site]["max"],
            )
            self.client[site] = Client(self.cluster[site], set_as_default=False)

    def get_cluster_url(self):
        for site in self.gateway_registry:
            print(self.cluster[site].dashboard_link)

    def shutdown(self):
        for site in self.gateway_registry:
            self.cluster[site].close()
