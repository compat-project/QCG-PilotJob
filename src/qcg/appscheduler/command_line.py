from qcg.appscheduler.service import QCGPMService


def service():
    QCGPMService().start()
