from concurrent.futures import Future
from qcg.pilotjob.api.manager import LocalManager


class QCGPJFuture(Future):

    qcgpjm: LocalManager

    def __init__(self, ids, qcgpjm):
        super().__init__()
        self.ids = ids
        self.qcgpjm = qcgpjm
        self._done = False
        self._cancelled = False

    def result(self, timeout=None):
        return self.qcgpjm.wait4(self.ids)

    def done(self):
        if self._done:
            return True

        statuses = self.qcgpjm.status(self.ids)
        for k in statuses['jobs'].keys():
            if not self.qcgpjm.is_status_finished(statuses['jobs'][k]['data']['status']):
                return False

        self._done = True
        return True

    def running(self):
        return not self.done()

    def cancel(self):
        if self._cancelled:
            return True

        self.qcgpjm.cancel(self.ids)
        self._cancelled = True
        return True

    def cancelled(self):
        return self._cancelled

