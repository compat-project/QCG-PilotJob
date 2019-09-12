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
        add( name = 'j1', exec = '/bin/date', stdout = 'j1.stdout' ).
        add( name = 'j2', exec = '/bin/hostname', args = [ '--fqdn'], stdout = 'j2.stdout')
        )

status = m.status(ids)
status = m.status('j1')

time.sleep(2)

status = m.status(ids)

m.wait4all()
#m.wait4(ids)
#info = m.info(ids)

m.remove(ids)

m.finish()

time.sleep(1)
