import uuid
from pathlib import Path


class DistributedScheduler:
    def __init__(self):
        self.host = "tcp://dedl.process-scheduler.eu:46045"

    @property
    def worker(self):
        return f"dedl-worker-{str(uuid.uuid1()).split('-')[0]}"

    def _repr_html_(self):
        return (Path(__file__).parent / 'templates/distributed_scheduler.html').read_text().format(host=self.host)

    def __repr__(self):
        return "<DistributedScheduler: tcp://dedl.process-scheduler.eu:46045>"
