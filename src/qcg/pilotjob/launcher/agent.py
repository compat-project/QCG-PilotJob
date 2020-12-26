import sys
import asyncio
import logging
import os
import socket
import json
from datetime import datetime
from os.path import join

import zmq
from zmq.asyncio import Context


_logger = logging.getLogger(__name__)


class Agent:
    """The node agent class.
    This class is responsible for launching jobs on local resources.

    Attributes:
        agent_id (str): agent identifier
        options (dict): agent options
        _finish (bool): true if agent should finish
        context (zmq.Context): ZMQ context
        in_socket (zmq.Socket): agent listening socket
        local_port (int): the local listening socket port
        local_address (str): the local listening socket address (proto://ip:port)
        remote_address (str): the launcher address
        local_export_address (str):
    """

    MIN_PORT_RANGE = 10000
    MAX_PORT_RANGE = 40000

    def __init__(self, a_id, opts):
        """Initialize instance.

        Args:
            a_id - agent identifier
            opt - agent options
        """
        self.agent_id = a_id
        self._finish = False

        self.options = opts

        # set default options
        self.options.setdefault('binding', False)

        _logger.info('agent options: %s', str(self.options))

        self.context = None
        self.in_socket = None
        self.local_port = None
        self.local_address = None
        self.remote_address = None
        self.local_export_address = None

    def _clear(self):
        """Reset runtime settings. """
        self.context = None
        self.in_socket = None
        self.local_port = None
        self.local_address = None
        self.remote_address = None
        self.local_export_address = None

    async def agent(self, remote_address, ip_addr='0.0.0.0', proto='tcp', min_port=MIN_PORT_RANGE,
                    max_port=MAX_PORT_RANGE):
        """The agent handler method.
        The agent will listen on local incoming socket until it receive the EXIT command.
        At the start, the agent will sent to the launcher (on launcher's remote address) the READY message
        with the local incoming address.

        Args:
            remote_address (str): the launcher address where information about job finish will be sent
            ip_addr (str): the local IP where incoming socket will be bineded
            proto (str): the protocol of the incoming socket
            min_port (int): minimum port number to listen on
            max_port (int): maximum port number to listen on
        """
        self.remote_address = remote_address
        self.context = Context.instance()

        _logger.debug('agent with id (%s) run to report to (%s)', self.agent_id, self.remote_address)

        self.in_socket = self.context.socket(zmq.REP) #pylint: disable=maybe-no-member

        laddr = '{}://{}'.format(proto, ip_addr)

        self.local_port = self.in_socket.bind_to_random_port(laddr, min_port=min_port, max_port=max_port)
        self.local_address = '{}:{}'.format(laddr, self.local_port)
        self.local_export_address = '{}://{}:{}'.format(proto, socket.gethostbyname(socket.gethostname()),
                                                        self.local_port)

        _logger.debug('agent with id (%s) listen at address (%s), export address (%s)',
                      self.agent_id, self.local_address, self.local_export_address)

        try:
            await self._send_ready()
        except Exception:
            _logger.error('failed to signaling ready to manager: %s', sys.exc_info())
            self._cleanup()
            self._clear()
            raise

        while not self._finish:
            message = await self.in_socket.recv_json()

            cmd = message.get('cmd', 'UNKNOWN')

            await self.in_socket.send_json({'status': 'OK'})

            cmd = message.get('cmd', 'unknown').lower()
            if cmd == 'exit':
                self._cmd_exit(message)
            elif cmd == 'run':
                self._cmd_run(message)
            else:
                _logger.error('unknown command received from launcher: %s', message)

        try:
            await self._send_finishing()
        except Exception as exc:
            _logger.error('failed to signal shuting down: %s', str(exc))

        self._cleanup()
        self._clear()

    def _cleanup(self):
        """Close sockets."""
        if self.in_socket:
            self.in_socket.close()

    def _cmd_exit(self, message):
        """Handler of EXIT command.

        Args:
            message - message from the launcher
        """
        _logger.debug('handling finish cmd with message (%s)', str(message))
        self._finish = True

    def _cmd_run(self, message):
        """Handler of RUN application command.

        Args:
            message - message with the following attributes:
                appid - application identifier
                args - the application arguments
        """
        _logger.debug('running app %s with args %s ...', message.get('appid', 'UNKNOWN'), str(message.get('args', [])))

        asyncio.ensure_future(self._launch_app(
            message.get('appid', 'UNKNOWN'),
            args=message.get('args', []),
            stdin=message.get('stdin', None),
            stdout=message.get('stdout', None),
            stderr=message.get('stderr', None),
            env=message.get('env', None),
            wdir=message.get('wdir', None),
            cores=message.get('cores', None)
        ))

    async def _launch_app(self, appid, args, stdin, stdout, stderr, env, wdir, cores):
        """Run application.

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
        stdin_p = None
        stdout_p = asyncio.subprocess.DEVNULL
        stderr_p = asyncio.subprocess.DEVNULL

        exit_code = -1

        starttime = datetime.now()

        try:
            if len(args) < 1:
                raise Exception('missing application executable')

            if cores and self.options.get('binding', False):
                app_exec = 'taskset'
                app_args = ['-c', ','.join([str(c) for c in cores]), *args]
            else:
                app_exec = args[0]
                app_args = args[1:] if len(args) > 1 else []

            _logger.info("creating process for job %s with executable (%s) and args (%s)",
                         appid, app_exec, str(app_args))
            _logger.debug("process env: %s", str(env))

            if stdin and wdir and not os.path.isabs(stdin):
                stdin = os.path.join(wdir, stdin)

            if stdout and wdir and not os.path.isabs(stdout):
                stdout = os.path.join(wdir, stdout)

            if stderr and wdir and not os.path.isabs(stderr):
                stderr = os.path.join(wdir, stderr)

            if stdin:
                stdin_p = open(stdin, 'r')

            if stdout and stderr and stdout == stderr:
                stdout_p = stderr_p = open(stdout, 'w')
            else:
                if stdout:
                    stdout_p = open(stdout, 'w')

                if stderr:
                    stderr_p = open(stderr, 'w')

            process = await asyncio.create_subprocess_exec(
                app_exec, *app_args, stdin=stdin_p, stdout=stdout_p, stderr=stderr_p, cwd=wdir, env=env)

            _logger.debug("process for job %s launched", appid)

            await process.wait()

            runtime = (datetime.now() - starttime).total_seconds()

            exit_code = process.returncode

            _logger.info("process for job %s finished with exit code %d", appid, exit_code)

            status_data = {
                'appid': appid,
                'agent_id': self.agent_id,
                'date': datetime.now().isoformat(),
                'status': 'APP_FINISHED',
                'ec': exit_code,
                'runtime': runtime}
        except Exception as exc:
            _logger.error('launching process for job %s finished with error - %s', appid, str(exc))
            status_data = {
                'appid': appid,
                'agent_id': self.agent_id,
                'date': datetime.now().isoformat(),
                'status': 'APP_FAILED',
                'message': str(exc)}
        finally:
            if stdin_p:
                stdin_p.close()
            if stdout_p != asyncio.subprocess.DEVNULL:
                stdout_p.close()
            if stderr_p not in (asyncio.subprocess.DEVNULL, stdout_p):
                stderr_p.close()

        out_socket = self.context.socket(zmq.REQ) #pylint: disable=maybe-no-member
        out_socket.setsockopt(zmq.LINGER, 0) #pylint: disable=maybe-no-member

        try:
            out_socket.connect(self.remote_address)

            await out_socket.send_json(status_data)
            msg = await out_socket.recv_json()
            _logger.debug("got confirmation for process finish %s", str(msg))
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except Exception:
                    pass

    async def _send_ready(self):
        """Send READY message to the launcher.
        The message will contain also the local listening address.
        """
        out_socket = self.context.socket(zmq.REQ) #pylint: disable=maybe-no-member
        out_socket.setsockopt(zmq.LINGER, 0) #pylint: disable=maybe-no-member

        try:
            out_socket.connect(self.remote_address)

            await out_socket.send_json({
                'status': 'READY',
                'date': datetime.now().isoformat(),
                'agent_id': self.agent_id,
                'local_address': self.local_export_address})

            msg = await out_socket.recv_json()

            _logger.debug('received ready message confirmation: %s', str(msg))

            if not msg.get('status', 'UNKNOWN') == 'CONFIRMED':
                _logger.error('agent %s not registered successfully in launcher: %s', self.agent_id, str(msg))
                raise Exception('not successfull registration in launcher: {}'.format(str(msg)))
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except Exception:
                    pass

    async def _send_finishing(self):
        """Send FINISHING message to the launcher.
        This message is the last message sent by the agent to the launcher before
        shuting down.
        """
        out_socket = self.context.socket(zmq.REQ) #pylint: disable=maybe-no-member
        out_socket.setsockopt(zmq.LINGER, 0) #pylint: disable=maybe-no-member

        _logger.debug("sending finishing message")
        try:
            out_socket.connect(self.remote_address)

            await out_socket.send_json({
                'status': 'FINISHING',
                'date': datetime.now().isoformat(),
                'agent_id': self.agent_id,
                'local_address': self.local_address})
            _logger.debug("finishing message sent, waiting for confirmation")
            msg = await out_socket.recv_json()

            _logger.debug('received finishing message confirmation: %s', str(msg))
        finally:
            if out_socket:
                try:
                    out_socket.close()
                except Exception:
                    pass


if __name__ == '__main__':
    if len(sys.argv) < 3 or len(sys.argv) > 5:
        print('error: wrong arguments\n\n\tagent {id} {remote_address} [options_in_json]\n\n')
        sys.exit(1)

    agent_id = sys.argv[1]
    raddress = sys.argv[2]
    options_arg = sys.argv[3] if len(sys.argv) > 3 else None

    options = {}
    if options_arg:
        try:
            options = json.loads(options_arg)
        except Exception as exc:
            print('failed to parse options: {}'.format(str(exc)))
            sys.exit(1)

    logging.basicConfig(
        level=logging.DEBUG,  # level=logging.INFO
        filename=join(options.get('auxDir', '.'), 'nl-agent-{}.log'.format(agent_id)),
        format='%(asctime)-15s: %(message)s')

    if asyncio.get_event_loop() and asyncio.get_event_loop().is_closed():
        asyncio.set_event_loop(asyncio.new_event_loop())

    agent = Agent(agent_id, options)

    asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(agent.agent(raddress)))
    asyncio.get_event_loop().close()

    _logger.info('node agent %s exiting', agent_id)
    sys.exit(0)
