import zmq
import time

from qcg.appscheduler.api.manager import Manager
from qcg.appscheduler.api.job import Jobs

#m = Manager("tcp://127.0.0.1:5555")
m = Manager()

print("available resources:\n%s\n" % str(m.resources()))
print("submited jobs:\n%s\n" % str(m.list().names()))

#j = Jobs()
#j.add( 'j1', { 'exec': '/bin/date' } )

ids = m.submit(Jobs().
        add( 'j_${it}', { 'iterate': [ 0, 100 ], 'exec': '/bin/sleep', 'args': [ '2s' ] } )
        )

#status = m.status(ids)

#time.sleep(2)

m.wait4all()
#m.wait4(ids)
#info = m.info(ids)

#m.remove(ids)

m.finish()

#time.sleep(1)
