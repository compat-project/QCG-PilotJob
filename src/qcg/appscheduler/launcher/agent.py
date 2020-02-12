import zmq
import sys
import asyncio
import logging
import os
import socket
import json
from datetime import datetime
from datetime import datetime
from os.path import join
from zmq.asyncio import Context



class Agent:

    MIN_PORT_RANGE = 10000
    MAX_PORT_RANGE = 40000

    def __init__(self, agent_id, options):
        """
        The node agent class.
        This class is responsible for launching jobs on local resources.

        Args:
            agent_id - agent identifier
            options - agent options
            context - ZMQ context
            in_socket - agent listening socket
            local_port - the local listening socket port
            local_address - the local listening socket address (proto://ip:port)
            remote_address - the launcher address
        """
        self.agent_id = agent_id
        self.__finish = False

        self.options = {}
        if options:
            try:
                self.options = json.loads(options)
            except Exception as e:
                logging.warning('failed to parse options: {}'.format(str(e)))

        if not self.options or not isinstance(self.options, dict):
            self.options = {}

        # set default options
        self.options.setdefault('binding', False)

        logging.info('agent options: {}'.format(str(self.options)))

        self.__clear()


    def __clear(self):
        """
        Reset runtime settings.
        """
        self.context = None
        self.in_socket = None
        self.local_port = None
        self.local_address = None
        self.remote_address = None


    async def agent(self, remote_address, ip='0.0.0.0', proto='tcp', min_port=MIN_PORT_RANGE, max_port=MAX_PORT_RANGE):
        """
        The agent handler method.
        The agent will listen on local incoming socket until it receive the EXIT command.
        At the start, the agent will sent to the launcher (on launcher's remote address) the READY message
        with the local incoming address.

        Args:
            remote_address - the launcher address where information about job finish will be sent
            ip - the local IP where incoming socket will be bineded
            proto - the protocol of the incoming socket
        """
        self.remote_address = remote_address
        self.context = Context.instance()

        logging.debug('agent with id ({}) run to report to ({})'.format(self.agent_id, self.remote_address))

        self.in_socket = self.context.socket(zmq.REP)

        laddr = '{}://{}'.format(proto, ip)

        self.local_port = self.in_socket.bind_to_random_port(laddr, min_port=min_port, max_port=max_port)
        self.local_address = '{}:{}'.format(laddr, self.local_port)
        self.local_export_address = '{}://{}:{}'.format(proto, socket.gethostbyname(socket.gethostname()), self.local_port)

        logging.debug('agent with id ({}) listen at address ({}), export address ({})'.format(self.agent_id, self.local_address, self.local_export_address))

        try:
            await self.__send_ready()
        except Exception:
            self.__cleanup()
            self.__clear()
            raise
        
        while not self.__finish:
            message = await self.in_socket.recv_json()

            cmd = message.get('cmd', 'UNKNOWN')

            await self.in_socket.send_json({ 'status': 'OK' })

            cmd = message.get('cmd', 'unknown').lower()
            if cmd == 'exit':
                self.__cmd_exit(message)
            elif cmd == 'run':
                self.__cmd_run(message)
            else:
                logging.error('unknown command received from launcher: {}'.format(message))

        try:
            await self.__send_finishing()
        except Exception as e:
            logger.error('failed to signal shuting down: {}'.format(str(e)))

        self.__cleanup()
        self.__clear()


    def __cleanup(self):
        if self.in_socket:
            self.in_socket.close()


    def __cmd_exit(self, message):
        """
        Handler of EXIT command.

        Args:
            message - message from the launcher
        """
        logging.debug('handling finish cmd with message ({})'.format(str(message)))
        self.__finish = True


    def __cmd_run(self, message):
        """
        Handler of RUN application command.

        Args:
            message - message with the following attributes:
                appid - application identifier
                args - the application arguments
        """
        logging.debug('running app {} with args {} ...'.format(message.get('appid', 'UNKNOWN'), str(message.get('args', []))))

        asyncio.ensure_future(self.__launch_app(
            message.get('appid', 'UNKNOWN'),
            args=message.get('args', []),
            stdin=message.get('stdin', None),
            stdout=message.get('stdout', None),
            stderr=message.get('stderr', None),
            env=message.get('env', None),
            wdir=message.get('wdir', None),
            cores=message.get('cores', None)
            ))


    async def __launch_app(self, appid, args, stdin, stdout, stderr, env, wdir, cores):
        """
        Run application.

        Args:
            appid - application identifier
            args - list of application command and it's arguments
            stdin - path to the standard input file
            stdout - path to the standard output file
            stderr - path to the standard error file
            env - environment variables
            wdir - working directory
            cores - a list of cores application should be binded to
        """
        stdinP = None
        stdoutP = asyncio.subprocess.DEVNULL
        stderrP = asyncio.subprocess.DEVNULL
        startedDt = datetime.now()

        exitCode = -1

        starttime = datetime.now()

        try:
            if len(args) < 1:
                raise Exception('missing application executable')

            if cores and self.options.get('binding', False):
                app_exec = 'taskset'
                app_args = [ '-c', ','.join([str(c) for c in cores]), *args ]
            else:
                app_exec = args[0]
                app_args = args[1:] if len(args) > 1 else [ ]

            logging.info("creating process for job {} with executable ({}) and args ({})".format(appid, app_exec, str(app_args)))
            logging.debug("process env: {}".format(str(env)))

            if stdin and wdir and not os.path.isabs(stdin):
                stdin = os.path.join(wdir, stdin)

            if stdout and wdir and not os.path.isabs(stdout):
                stdout = os.path.join(wdir, stdout)

            if stderr and wdir and not os.path.isabs(stderr):
                stderr = os.path.join(wdir, stderr)


            if stdin:
                stdinP = open(stdin, 'r')

            if stdout and stderr and stdout == stderr:
                stdoutP = stderrP = open(stdout, 'w')
            else:
                 if stdout:
                     stdoutP = open(stdout, 'w')

                 if stderr:
                     stderrP = open(stderr, 'w')

            process = await asyncio.create_subprocess_exec(app_exec, *app_args,
                    stdin=stdinP, stdout=stdoutP, stderr=stderrP, cwd=wdir, env=env)

            logging.debug("process for job {} launched".format(appid))

            await process.wait()

            runtime = (datetime.now() - starttime).total_seconds()

            exitCode = process.returncode
        
            logging.info("process for job {} finished with exit code {}".format(appid, exitCode))

            status_data = {
                    'appid': appid,
                    'agent_id': self.agent_id,
                    'date': datetime.now().isoformat(),
                    'status': 'APP_FINISHED',
                    'ec': exitCode, 'runtime': runtime }
        except Exception as e:
            logging.error('launching process for job {} finished with error - {}'.format(appid, str(e)))
            status_data = {
                    'appid': appid,
                    'agent_id': self.agent_id,
                    'date': datetime.now().isoformat(),
                    'status': 'APP_FAILED',
                    'message': str(e) }
        finally:
            if stdinP:
                stdinP.close()
            if stdoutP != asyncio.subprocess.DEVNULL:
                stdoutP.close()
            if stderrP != asyncio.subprocess.DEVNULL and stderrP != stdoutP:
                stderrP.close()

        out_socket = self.context.socket(zmq.REQ)
        try:
            out_socket.connect(self.remote_address)

            await out_socket.send_json(status_data)
            msg = await out_socket.recv_json()
            logging.debug("got confirmation for process finish {}".format(str(msg)))
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except:
                    pass


    async def __send_ready(self):
        """
        Send READY message to the launcher.
        The message will contain also the local listening address.
        """
        out_socket = self.context.socket(zmq.REQ)
        try:
            out_socket.connect(self.remote_address)

            await out_socket.send_json({
                'status': 'READY',
                'date': datetime.now().isoformat(),
                'agent_id': self.agent_id,
                'local_address': self.local_export_address })

            msg = await out_socket.recv_json()

            logging.debug('received ready message confirmation: {}'.format(str(msg)))

            if not msg.get('status', 'UNKNOWN') == 'CONFIRMED':
                logger.error('agent {} not registered successfully in launcher: {}'.format(self.agent_id, str(msg)))
                raise Exception('not successfull registration in launcher: {}'.format(str(msg)))
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except:
                    pass


    async def __send_finishing(self):
        """
        Send FINISHING message to the launcher.
        This message is the last message sent by the agent to the launcher before
        shuting down.
        """
        out_socket = self.context.socket(zmq.REQ)
        try:
            out_socket.connect(self.remote_address)

            await out_socket.send_json({
                'status': 'FINISHING',
                'date': datetime.now().isoformat(),
                'agent_id': self.agent_id,
                'local_address': self.local_address })
            msg = await out_socket.recv_json()

            logging.debug('received finishing message confirmation: {}'.format(str(msg)))
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except:
                    pass


if __name__ == '__main__':
    if len(sys.argv) < 3 or len(sys.argv) > 5:
        print('error: wrong arguments\n\n\tagent {id} {remote_address} [options_in_json]\n\n')
        sys.exit(1)

    agent_id, raddress = sys.argv[1:3]
    options = sys.argv[3] if len(sys.argv) > 3 else None
    
    logging.basicConfig(
            level=logging.INFO,
            filename=join('.qcgpjm', 'nl-agent-{}.log'.format(agent_id)),
            format='%(asctime)-15s: %(message)s')

    agent = Agent(agent_id, options)

    asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(agent.agent(raddress)))

    logging.error('node agent {} exiting'.format(agent_id))
