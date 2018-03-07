import zmq

from qcg.appscheduler.api.manager import Manager

m = Manager("tcp://127.0.0.1:5555")

print("available resources:\n%s\n" % str(m.resources()))
print("submited jobs:\n%s\n" % str(m.list()))
