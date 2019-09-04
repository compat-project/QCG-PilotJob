import zmq
import sys
import asyncio
import json
import sys
import logging
import shutil
import socket
import os
from datetime import datetime
from zmq.asyncio import Context


class Launcher:

    MIN_PORT_RANGE = 10000
    MAX_PORT_RANGE = 40000
    START_TIMEOUT_SECS = 20

    def __init__(self):
        # local context
        self.zmq_ctx = {}

        # requested agent instances
        self.agents = {}

        # registered agent instances
        self.nodes = {}

        # application finish default callback
        self.jobs_def_cb = None

        # application finish callbacks
        self.jobs_cb = {}

        # only for tests
        self.app_finished = 0

        self.node_local_agent_cmd = [ sys.executable, '-m', 'qcg.appscheduler.launcher.agent' ]
        self.node_ssh_agent_cmd = [ 'cd {}; {} -m qcg.appscheduler.launcher.agent'.format(os.getcwd(), sys.executable) ]


    def set_job_finish_callback(self, jobs_finish_cb, *jobs_finish_cb_args):
        """
        Set default function for notifing about finished jobs.

        Args:
            jobs_finish_cb - optional default job finish callback
            jobs_finish_cb_args - job finish callback parameters
        """
        if jobs_finish_cb:
            self.jobs_def_cb = { 'f': jobs_finish_cb, 'args': jobs_finish_cb_args }
        else:
            self.jobs_def_cb = None


    async def start(self, instances, local_port=None):
        """
        Initialize launcher with given agent instances.
        The instances data must contain list of places along with the data needed to
        initialize instance services.

        Args:
            instances ({} []) - list of dictionaries where each element must contain fields:
                agent_id - the agent identifier
                ssh - if the instance should be run via ssh (account?,host?,remote_python?)
                slurm - if the instance should be run via slurm ()
                local - if the instance should be run on a local machine
            local_port - optional local port for the incoming connections, if not defined,
                the available random port will be chosen from the range
        """
        try:
            await self.__init_input_iface(local_port=local_port)
            logging.debug('local manager listening at {}'.format(self.local_address))

            await self.__fire_agents(instances)
        except Exception as e:
            logging.error('failed to start agents: {}'.format(str(e)))
            await self.__cleanup()
            raise


    async def stop(self):
        await self.__cleanup()


    async def submit(self, agent_id, app_id, args, stdin=None, stdout=None, stderr=None, env=None, wdir=None, cores=None,
            finish_cb=None, finish_cb_args=None):
        """
        Submit application to be launched by the selected agent.

        Args:
            agent_id - agent that should launch applicaton
            app_id - application identifier
            args - aplication arguments
            stdin - path to the standard input file
            stdout - path to the standard output file
            stderr - path to the standard error file
            env - environment variables (if not defined, the current environment is used)
            wdir - working directory (if not defined, the current working directory is used)
            cores - the list of cores application should be binded to (if not defined, there will be no binding)
            finish_cb - job finish callback
            finish_cb_args - job finish callback arguments
        """
        if not agent_id in self.agents:
            logger.error('agent {} not registered'.format(agent_id))
            raise Exception('agent {} not registered'.format(agent_id))

        if finish_cb:
            self.jobs_cb[app_id] = { 'f': finish_cb, 'args': finish_cb_args }

        agent = self.nodes[agent_id]

        out_socket = self.zmq_ctx.socket(zmq.REQ)
        out_socket.connect(agent['address'])

        await out_socket.send_json({
            'cmd': 'RUN',
            'appid': app_id,
            'args': args,
            'stdin': stdin,
            'stdout': stdout,
            'stderr': stderr,
            'env': env,
            'wdir': wdir,
            'cores': cores })
        msg = await out_socket.recv_json()

        if not msg.get('status', None) == 'OK':
            logging.error('failed to run application {} by agent {}: {}'.format(app_id, agent_id, str(msg)))

            if finish_cb:
                del self.jobs_cb[app_id]

            raise Exception('failed to run application {} by agent {}: {}'.format(app_id, agent_id, str(msg)))
        else:
            logging.debug('application {} successfully launched by agent {}'.format(app_id, agent_id))


    async def __cleanup(self):
        """
        Release all resources.
        Stop all agents, cancel all async tasks and close sockets.
        """

        # signal node agent to shutdown
        try:
            await self.__shutdown_agents()
        except Exception:
            pass

        # kill agent processes
        self.__cancel_agents()

        # cancel input interface task
        if self.iface_task:
            self.iface_task.cancel()

            try:
                await self.iface_task
            except asyncio.CancelledError:
                logging.debug('input interface task finished')

        # close input interface socket
        if self.in_socket:
            self.in_socket.close()


    async def __shutdown_agents(self):
        """
        Signal all running node agents to shutdown and wait for notification about finishing.
        """
        out_socket = self.zmq_ctx.socket(zmq.REQ)

        # create copy because elements might be removed when signal finishing
        nodes = self.nodes.copy()

        for agent_id, agent in nodes.items():
            logging.debug('connecting to node agent {} @ {} ...'.format(agent_id, agent['address']))

            try:
                out_socket.connect(agent['address'])

                await out_socket.send_json({ 'cmd': 'EXIT' })

                msg = await out_socket.recv_json()

                if not msg.get('status', None) == 'OK':
                    logging.error('failed to finish node agent {}: {}'.format(agent_id, str(msg)))
                else:
                    logging.debug('node agent {} signaled to shutdown'.format(agent_id))

                out_socket.disconnect(agent['address'])
            except Exception as e:
                logging.error('failed to signal agent to shutdown: {}'.format(str(e)))

        start_t = datetime.now()

        error = False
        while len(self.nodes) > 0:
            if (datetime.now() - start_t).total_seconds() > Launcher.SHUTDOWN_TIMEOUT_SECS:
                logging.error('timeout while waiting for agents exit - currenlty not finished {}: {}'.format(len(self.nodes), str(','.join(self.nodes.keys()))))
                raise Exception('timeout while waiting for agents exit')

            await asyncio.sleep(0.2)


    def __cancel_agents(self):
        """
        Kill agent processes.
        """
        for agent_id, agent in self.agents.items():
            if agent.get('process', None):
                logging.debug('killing agent {} ...'.format(agent_id))
                try:
                    agent['process'].kill()
                except:
                    pass

        self.agents.clear()


    async def __fire_agents(self, instances):
        """
        Launch node agent instances.
        The input interface must be running to get notification about started agents.
        """
        for idata in instances:
            if not all(('agent_id' in idata, 'ssh' in idata or 'slurm' in idata or 'local' in idata)):
                raise ValueError('insufficient agent instance data: {}'.format(str(idata)))

            if idata['agent_id'] in self.agents:
                raise KeyError('agnet instance {} already defined'.format(idata['agent_id']))

            proto = 'ssh' if 'ssh' in idata else None
            proto = 'slurm' if 'slurm' in idata else None
            if not proto:
                proto = 'local'
            
            logging.info('running node agent {} via {}'.format(idata['agent_id'], proto))

            if 'ssh' in idata:
                process = await self.__fire_ssh_agent(idata['ssh'], [ idata['agent_id'], self.local_export_address ])
            elif 'slurm' in idata:
                process = await self.__fire_slurm_agent(idata['slurm'], [ idata['agent_id'], self.local_export_address ])
            elif 'local' in idata:
                process = await self.__fire_local_agent(idata['local'], [ idata['agent_id'], self.local_export_address ])
            else:
                raise ValueError('missing start type of node agent {}'.format(idata['agent_id'] ))

            self.agents[idata['agent_id']] = { 'process': process, 'data': idata }


        start_t = datetime.now()

        while len(self.nodes) < len(self.agents):
            if (datetime.now() - start_t).total_seconds() > Launcher.START_TIMEOUT_SECS:
                logging.error('timeout while waiting for agents - currenlty registered {} from launched {}'.format(len(self.nodes), len(self.agents)))
                raise Exception('timeout while waiting for agents')

            await asyncio.sleep(0.2)

        logging.debug('all agents {} registered in {} seconds'.format(len(self.nodes), (datetime.now() - start_t).total_seconds()))


    async def __fire_ssh_agent(self, ssh_data, args):
        """
        Launch node agent instance via ssh.

        Args:
            ssh_data (dict) - must contain 'host' attribute, optional attributes: 'account' (str), 'args' (str[])
            args - aguments for node agent application
        """
        if not 'host' in ssh_data:
            raise ValueError('missing ssh host definition')

        ssh_address = ssh_data['host']
        if 'account' in ssh_data:
            ssh_address = '{}@{}'.format(ssh_data['account'], ssh_address)

        ssh_args = ssh_data.get('args', [])

        return await asyncio.create_subprocess_exec(shutil.which('ssh'), ssh_address, *ssh_args,
                *self.node_ssh_agent_cmd, *args)


    async def __fire_slurm_agent(self, slurm_data, args):
        """
        Launch node agent instance via slurm (inside allocation).

        Args:
            slurm_data (dict) - must contain 'node' attribute, optional attributes: 'node' (str), 'args' (str[])
            args - aguments for node agent application
        """
        if not 'node' in slurm_data:
            raise ValueError('missing slurm node name')

        slurm_args = [ '-w', slurm_data['node'], '-N', '1', '-n', '1', '-D', os.getcwd() ]
        slurm_args.extend(slurm_data.get('args', []))

        return await asyncio.create_subprocess_exec(shutil.which('srun'), *slurm_args,
                *self.node_local_agent_cmd, *args)


    async def __fire_local_agent(self, local_data, args):
        """
        Launch node agent instance locally.

        Args:
            local_data (dict) - 
            args - aguments for node agent application
        """
        return await asyncio.create_subprocess_exec(*self.node_local_agent_cmd, *args)


    async def __init_input_iface(self, local_port=None, proto='tcp', ip='0.0.0.0', port_min=MIN_PORT_RANGE, port_max=MAX_PORT_RANGE):
        """
        Initialize input interface.
        The ZMQ socket is created and binded for incoming notifications (from node agents).

        Args:
            local_port - optional local port to bind to
            proto - optional protocol for ZMQ socket
            ip - optional local IP address
            port_min - optional minimum range port number if local_port has not been defined
            port_max - optional maximum range port number if local port has not been defined
        """
        self.zmq_ctx = Context.instance()
        self.in_socket = self.zmq_ctx.socket(zmq.REP)

        if local_port:
            address = '{}://{}:{}'.format(proto, ip, local_port)
            port = self.in_socket.bind(address)
        else:
            addr = '{}://{}'.format(proto, ip)
            port = self.in_socket.bind_to_random_port(addr, min_port=port_min, max_port=port_max)
            address = '{}:{}'.format(addr, port)

        self.local_address = address
        self.local_export_address = '{}://{}:{}'.format(proto, socket.gethostbyname(socket.gethostname()), port)

        logging.debug('local address accessible by other machines: {}'.format(self.local_export_address))

        self.iface_task = asyncio.ensure_future(self.__input_iface())
                

    async def __input_iface(self):
        """
        The handler for input interface.
        The incoming notifications are processed and launcher state is modified.
        """
        while True:
            msg = await self.in_socket.recv_json()

            logging.debug('received message: {}'.format(str(msg)))

            if msg.get('status', None) == 'READY':
                try:
                    if not all(('agent_id' in msg, 'local_address' in msg)):
                        raise Exception('missing identifier/local address in ready message')
                except Exception as e:
                    logging.error('error: {}'.format(str(e)))
                    await self.in_socket.send_json({ 'status': 'ERROR', 'message': str(e) })
                    next
                else:
                    await self.in_socket.send_json({ 'status': 'CONFIRMED'})

                self.nodes[msg['agent_id']] = { 'registered_at': datetime.now(), 'address': msg['local_address'] }
                logging.debug('registered at ({}) agent ({}) listening at ({})'.format(self.nodes[msg['agent_id']]['registered_at'], msg['agent_id'], self.nodes[msg['agent_id']]['address']))
            elif msg.get('status', None) in ['APP_FINISHED', 'APP_FAILED']:
                await self.in_socket.send_json({ 'status': 'CONFIRMED'})

                appid = msg.get('appid', 'UNKNOWN')

                logging.debug('received notification with application {} finish {}'.format(appid, str(msg)))

                if appid in self.jobs_cb:
                    try:
                        args = (*self.jobs_cb[appid]['args'], msg) if self.jobs_cb[appid]['args'] else (msg)
                        logging.debug('calling application finish callback with object {} and args {}'.format(self.jobs_cb[appid]['f'], str(args)))
                        self.jobs_cb[appid]['f'](args)
                    except Exception as e:
                        logging.error('failed to call application finish callback: {}'.format(str(e)))

                    del self.jobs_cb[appid]
                elif self.jobs_def_cb:
                    logging.debug('calling application default finish callback with object {}'.format(self.jobs_def_cb['f']))
                    try:
                        args = (*self.jobs_def_cb['args'], msg) if self.jobs_def_cb['args'] else (msg)
                        self.jobs_def_cb['f'](args)
                    except Exception as e:
                        logging.error('failed to call application default finish callback: {}'.format(str(e)))
                else:
                    logging.debug('NOT calling application finish callback')
            elif msg.get('status', None) == 'FINISHING':
                await self.in_socket.send_json({ 'status': 'CONFIRMED'})

                logging.debug('received notification with node agent finish: {}'.format(str(msg)))

                if 'agent_id' in msg and msg['agent_id'] in self.nodes:
                    logging.debug('removing node agent {}'.format(msg['agent_id']))
                    del self.nodes[msg['agent_id']]
                else:
                    logging.error('node agent {} notifing FINISH not known'.format(msg.get('agent_id', 'UNKNOWN')))
            else:
                await self.in_socket.send_json({ 'status': 'ERROR'})


def finish_callback_default(text, message):
    print('got default notifinication with job finish ({}): {}'.format(text, str(message)))

def finish_callback(text, jobid, message):
    print('got notifinication with job {} finish ({}): {}'.format(jobid, text, str(message)))


async def run_job(launcher, agent_id, appid, args, stdin=None, stdout=None, stderr=None, env=None):
#    await launcher.submit(agent_id, appid, args, wdir='tmp-dir', finish_cb=finish_callback, finish_cb_args=['job finish callback', 'job_ID'])
    await launcher.submit(agent_id, appid, args, stdin=stdin, stdout=stdout, stderr=stderr, env=env, wdir='tmp-dir', cores=[0,1], finish_cb=finish_callback, finish_cb_args=['job finish callback', 'job_ID'])
#    await launcher.submit(agent_id, appid, args)


async def test():
    logging.basicConfig(level=logging.DEBUG)

    l = Launcher()

    l.set_job_finish_callback(finish_callback_default, 'job finish callback')

#    await l.start([{ 'agent_id': 'n1', 'local': { }}])
#    await l.start([{ 'agent_id': 'n1', 'ssh': { 'host': 'agave13' }}])
    await l.start([
        { 'agent_id': 'n1', 'slurm': { 'node': 'e0101' }},
        { 'agent_id': 'n2', 'slurm': { 'node': 'e0103' }}])

    try:
        await run_job(l, 'n1', 'job1', [ '/usr/bin/cat' ], stdin='/etc/system-release', stdout='env.out', stderr='env.err', env={'v1': 'V1_value', 'v2': ''})
        await run_job(l, 'n2', 'job2', [ '/usr/bin/cat' ], stdin='/etc/system-release', stdout='env2.out', stderr='env2.err', env={'v1': 'V1_value', 'v2': ''})
    except Exception as e:
        logger.error(str(e))

    await asyncio.sleep(5)

    await l.stop()


if __name__ == '__main__':
#    if len(sys.argv) > 2:
#        print('error: wrong arguments\n\n\tlauncher {local_address}\n\n')
#        sys.exit(1)

    asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(test()))
