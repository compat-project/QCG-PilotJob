from qcg.appscheduler.api.manager import Manager


m = Manager()

print("available resources:\n%s\n" % str(m.resources()))
print("submited jobs:\n%s\n" % str(m.list()))