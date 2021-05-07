from concurrent.futures import Future
from qcg.pilotjob.api.manager import LocalManager


class QCGPJFuture(Future):

    qcgpjm: LocalManager

    def __init__(self, ids, qcgpjm):
        super().__init__()
        self.ids = ids
        self.qcgpjm = qcgpjm

    def result(self, timeout=None):
        return self.qcgpjm.wait4(self.ids)
