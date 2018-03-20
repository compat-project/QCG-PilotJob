import zmq
import time

from qcg.appscheduler.api.manager import Manager
from qcg.appscheduler.api.job import Jobs

#m = Manager("tcp://127.0.0.1:5555")
m = Manager( cfg = { 'log_level': 'DEBUG', 'poll_delay': 1 } )

print("available resources:\n%s\n" % str(m.resources()))
print("submited jobs:\n%s\n" % str(m.list()))

#j = Jobs()
#j.add( 'j1', { 'exec': '/bin/date' } )

ids = m.submit(Jobs().
        add( name = 'msleep2', exec = '/bin/env', stdout = 'env.stdout' ).
        add( name = 'echo', exec = '/bin/date', stdout = 'date.stdout' )
        )

m.list()
m.status(ids)
m.info(ids)

m.wait4(ids)

m.remove(ids)

m.finish()
