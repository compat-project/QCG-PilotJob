#!/bin/env python

from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.contrib.completers import WordCompleter
from prompt_toolkit import prompt_async

from os.path import exists, abspath

import asyncio
import click
import re
import traceback
import zmq
from zmq.asyncio import Context
import json
import logging
import sys

CommandCompleter = WordCompleter(['jobs', 'status', 'jinfo', 'load', 'edit',
                                  'jcancel', 'jdel', 'connect', 'exit', 'finish', 'help'], ignore_case=True)


class Ctx:

    def __init__(self):
        self.zmqCtx = Context.instance()
        self.zmqSock = None
        self.connected = False
        self.finish = False

    async def waitForFinish(self):
        while not self.finish:
            await asyncio.sleep(1)


StatusSortValues = {
    'executing': 6,
    'queued': 5,
    'succeed': 4,
    'failed': 3,
    'canceled': 3,
    'omitted': 2,
    'unknown': 1
}


class CmdJobs:
    NAME = 'jobs'

    def __init__(self, args):
        if args is not None and len(args) > 0:
            raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, CmdJobs.help_str()))

    async def run(self, ctx):
        if not ctx.connected:
            raise Exception('not connected')

        await ctx.zmqSock.send(str.encode(json.dumps({
            "request": "listJobs"
        })))

        logging.info("request sent - waiting for response")

        reply = bytes.decode(await ctx.zmqSock.recv())
        logging.info("received reply: %s" % reply)
        d = json.loads(reply)

        if not isinstance(d, dict) or 'code' not in d or 'data' not in d:
            raise Exception('invalid reply from the service')

        if d['code'] != 0:
            if 'message' in d:
                raise Exception(d['message'])
            else:
                raise Exception('failed to get list of jobs')

        if 'jobs' not in d['data']:
            raise Exception('invalid reply from the service')

        jobs = []
        for job in d['data']['jobs']:
            jStatus = 'UNKNOWN'
            jMessages = None
            inQueue = sys.maxsize

            # optional
            if 'messages' in job:
                jMessages = job['messages']

            # optional
            if 'inQueue' in job:
                inQueue = job['inQueue']

            jobs.append({'name': job['name'],
                         'status': job['status'],
                         'messages': jMessages,
                         'inQueue': inQueue})

        # sort by position in queue
        almostSortedJobs1 = sorted(jobs,
                                   key=lambda job: job['inQueue'])

        # sort by status
        sortedJobs = sorted(almostSortedJobs1,
                            key=lambda job: StatusSortValues.get(job['status'].lower(), 'unknown'),
                            reverse=True)

        status = []
        for job in sortedJobs:
            if 'messages' in job and job['messages']:
                #				status.append("%s (%s, %s) - %d" % (job['name'], job['status'], job['messages'], job['inQueue']))
                status.append("%s (%s, %s)" % (job['name'], job['status'], job['messages']))
            else:
                #				status.append("%s (%s) - %d" % (job['name'], job['status'], job['inQueue']))
                status.append("%s (%s)" % (job['name'], job['status']))

        statusStr = '\n'.join(status)

        click.secho(statusStr, fg='green')

    @classmethod
    def description(cls):
        return '''List submited jobs'''

    @classmethod
    def help_str(cls):
        return '''%s syntax:\n\t%s''' % (cls.NAME, cls.NAME)


class CmdStatus:
    NAME = 'status'

    def __init__(self, args):
        if args is not None and len(args) > 0:
            raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, CmdStatus.help_str()))

    async def run(self, ctx):
        if not ctx.connected:
            raise Exception('not connected')

        await ctx.zmqSock.send(str.encode(json.dumps({
            "request": "resourcesInfo"
        })))

        logging.info("request sent - waiting for response")

        reply = bytes.decode(await ctx.zmqSock.recv())
        logging.info("received reply: %s" % reply)
        d = json.loads(reply)

        if not isinstance(d, dict) or 'code' not in d or 'data' not in d:
            raise Exception('invalid reply from the service')

        if d['code'] != 0:
            raise Exception('failed to get resource info')

        data = d['data']
        status = "nodes: %d, cores: %d (used %d - %.0f%%)" % (
            data['totalNodes'], data['totalCores'], data['usedCores'],
            round((data['usedCores'] / data['totalCores']) * 100.0))
        click.secho(status, fg='green')

    @classmethod
    def description(cls):
        return '''Display pilot manager resources status'''

    @classmethod
    def help_str(cls):
        return '''%s syntax:\n\t%s''' % (cls.NAME, cls.NAME)


class CmdJinfo:
    NAME = 'jinfo'

    def __init__(self, args):
        if args is None or len(args) != 1:
            raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, CmdJinfo.help_str()))

        self.jName = args[0]

    async def run(self, ctx):
        if not ctx.connected:
            raise Exception('not connected')

        await ctx.zmqSock.send(str.encode(json.dumps({
            "request": "jobStatus",
            "jobName": self.jName
        })))

        logging.info("request sent - waiting for response")

        reply = bytes.decode(await ctx.zmqSock.recv())
        logging.info("received reply: %s" % reply)
        d = json.loads(reply)

        if not isinstance(d, dict) or 'code' not in d:
            raise Exception('invalid reply from the service')

        if d['code'] != 0:
            if 'message' in d:
                raise Exception(d['message'])
            else:
                raise Exception('failed to get job status info')

        if 'data' not in d:
            raise Exception('invalid reply from the service')

        data = d['data']
        status = "%s" % (data['status'])

        if 'messages' in data:
            status = ' '.join([status, data['messages']])

        if 'history' in data:
            status = '\n'.join([status, data['history']])

        if 'runtime' in data and isinstance(data['runtime'], dict):
            for rk, rv in data['runtime'].items():
                status = '\n'.join([status, '%s: %s' % (rk, rv)])

        color = 'blue'
        if data['status'] in ['FAILED', 'CANCELED', 'OMITTED']:
            color = 'red'
        elif data['status'] in ['SUCCEED']:
            color = 'green'

        click.secho(status, fg=color)

    @classmethod
    def description(cls):
        return '''Display information about submited job'''

    @classmethod
    def help_str(cls):
        return '''%s syntax:\n\t%s {job name}''' % (cls.NAME, cls.NAME)


class CmdLoad:
    NAME = 'load'

    def __init__(self, args):
        if args is None or len(args) != 1:
            raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, CmdLoad.help_str()))

        self.reqFile = args[0]
        if not exists(self.reqFile):
            raise Exception('"%s" doesn\'t exists', self.reqFile)

    async def run(self, ctx):
        print()
        click.secho("loading requests from %s ..." % self.reqFile, fg='blue')

        reqData = None
        with open(self.reqFile) as jsonData:
            reqData = json.load(jsonData)

        if not isinstance(reqData, dict) or 'request' not in reqData:
            raise Exception('"%s" contains wrong format - missing single request definition')

        await ctx.zmqSock.send(str.encode(json.dumps(reqData)))

        logging.info("request sent - waiting for response")

        reply = bytes.decode(await ctx.zmqSock.recv())
        logging.info("received reply: %s" % reply)
        d = json.loads(reply)

        if not isinstance(d, dict) or 'code' not in d:
            raise Exception('invalid reply from the service')

        if d['code'] != 0:
            if 'message' in d:
                raise Exception(d['message'])
            else:
                raise Exception('request failed')

        status = []
        for k, v in d.items():
            if k == 'code':
                continue
            status.append('%s: %s' % (k, v))
        statusStr = '\n'.join(status)

        color = 'blue'
        color = 'green'

        click.secho(statusStr, fg=color)

    @classmethod
    def description(cls):
        return '''Load requests from file and send to manager'''

    @classmethod
    def help_str(cls):
        return '''%s syntax:\n\t%s {requests file}''' % (cls.NAME, cls.NAME)


class CmdJcancel:
    NAME = 'jcancel'

    def __init__(self, args):
        if args is None or len(args) != 1:
            raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, CmdJcancel.help_str()))

        self.jName = args[0]

    async def run(self, ctx):
        print("canceling job '%s' ..." % self.jName)

    @classmethod
    def description(cls):
        return '''Cancel submited job'''

    @classmethod
    def help_str(cls):
        return '''%s syntax:\n\t%s {job name}''' % (cls.NAME, cls.NAME)


class CmdJdel:
    NAME = 'jdel'

    def __init__(self, args):
        if args is None or len(args) != 1:
            raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, CmdJdel.help_str()))

        self.jName = args[0]

    async def run(self, ctx):
        if not ctx.connected:
            raise Exception('not connected')

        await ctx.zmqSock.send(str.encode(json.dumps({
            "request": "removeJob",
            "jobName": self.jName
        })))

        logging.info("request sent - waiting for response")

        reply = bytes.decode(await ctx.zmqSock.recv())
        logging.info("received reply: %s" % reply)
        d = json.loads(reply)

        if not isinstance(d, dict) or 'code' not in d:
            raise Exception('invalid reply from the service')

        if d['code'] != 0:
            if 'message' in d:
                raise Exception(d['message'])
            else:
                raise Exception('failed to get job status info')

        if 'data' in d and 'messages' in d['data']:
            click.secho(d['data']['messages'], fg='green')

    @classmethod
    def description(cls):
        return '''Remove finished job'''

    @classmethod
    def help_str(cls):
        return '''%s syntax:\n\t%s {job name}''' % (cls.NAME, cls.NAME)


class CmdConnect:
    NAME = 'connect'
    DEFAULT_ADDRESS = "tcp://127.0.0.1:5555"
    DEFAULT_PROTO = "tcp"
    DEFAULT_PORT = "5555"

    def __init__(self, args):
        if args is not None and len(args) > 1:
            raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, CmdConnect.help_str()))

        if args is None or len(args) < 1:
            self.address = CmdConnect.DEFAULT_ADDRESS
        else:
            address = args[0]

            if not re.match('\w*://', address):
                # append default protocol
                address = "%s://%s" % (CmdConnect.DEFAULT_PROTO, address)

            if not re.match('.*:\d+', address):
                # append default port
                address = "%s:%s" % (address, CmdConnect.DEFAULT_PORT)

            self.address = address

    async def run(self, ctx):
        if ctx.connected:
            click.secho("closing current connection ...", fg='blue')
            ctx.zmqSock.close()
            ctx.connected = False

        click.secho("connecting to the '%s' ..." % self.address, fg='blue')

        try:
            ctx.zmqSock = ctx.zmqCtx.socket(zmq.REQ)
            ctx.zmqSock.connect(self.address)
            ctx.connected = True
        except Exception as e:
            raise Exception('failed to connect to %s - %s' % (self.address, e.args[0]))

        click.secho("connection established", fg='green')

    @classmethod
    def description(cls):
        return '''Connect to pilot manager'''

    @classmethod
    def help_str(cls):
        return '''%s syntax:\n\t%s {address}''' % (cls.NAME, cls.NAME)


class CmdExit:
    NAME = 'exit'

    def __init__(self, args):
        if args is not None and len(args) > 0:
            raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, CmdExit.help_str()))

    async def run(self, ctx):
        click.secho("exiting ...", fg='green')

        if ctx.connected:
            ctx.zmqSock.close()
            ctx.connected = False
        ctx.finish = True

    @classmethod
    def description(cls):
        return '''Exit client'''

    @classmethod
    def help_str(cls):
        return '''%s syntax:\n\t%s''' % (cls.NAME, cls.NAME)


class CmdFinish:
    NAME = 'finish'

    def __init__(self, args):
        if args is not None and len(args) > 0:
            raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, CmdFinish.help_str()))

    async def run(self, ctx):
        if not ctx.connected:
            raise Exception('not connected')

        await ctx.zmqSock.send(str.encode(json.dumps({
            "request": "finish"
        })))

        logging.info("request sent - waiting for response")

        reply = bytes.decode(await ctx.zmqSock.recv())
        logging.info("received reply: %s" % reply)
        d = json.loads(reply)

        if not isinstance(d, dict) or 'code' not in d:
            raise Exception('invalid reply from the service')

        if d['code'] != 0:
            raise Exception('failed to finish service')

        click.secho('finish request sent', fg='green')

    @classmethod
    def description(cls):
        return '''Send finish signal to the pilot manager'''

    @classmethod
    def help_str(cls):
        return '''%s syntax:\n\t%s''' % (cls.NAME, cls.NAME)


class CmdHelp:
    NAME = 'help'

    def __init__(self, args):
        if args is not None and len(args) > 1:
            raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, CmdHelp.help_str()))

        self.cmd = None

        if args is not None and len(args) > 0:
            self.cmd = args[0]

    async def run(self, ctx):
        if self.cmd is None:
            await self._print_general_help()
        else:
            await self._print_cmd_help(self.cmd)

    async def _print_general_help(self):
        for cmd in Cmd.CMDS:
            click.secho("%s - %s" % (cmd.NAME, cmd.description()), fg='green')
            click.secho("\t%s" % (cmd.help_str().replace('\t', '\t\t')), fg='green')
            click.secho("", fg='green')

    async def _print_cmd_help(self, cmdName):
        if cmdName not in Cmd.CMDS_Names:
            raise Exception("Command '%s' unknown" % cmdName)

        cmd = Cmd.CMDS_Names[cmdName]
        click.secho("%s - %s" % (cmd.NAME, cmd.description()), fg='green')
        click.secho("\t%s" % (cmd.help_str().replace('\t', '\t\t')), fg='green')
        click.secho("", fg='green')

    @classmethod
    def description(cls):
        return '''Display help information'''

    @classmethod
    def help_str(cls):
        return '''%s syntax:\n\t%s {job name}''' % (cls.NAME, cls.NAME)


class Cmd:
    CMDS = [
        CmdJobs,
        CmdStatus,
        CmdJinfo,
        CmdLoad,
        CmdJcancel,
        CmdJdel,
        CmdConnect,
        CmdExit,
        CmdFinish,
        CmdHelp
    ]

    CMDS_Names = {}

    def __init__(self):
        CMDS_Names = {}

        for cmd in Cmd.CMDS:
            Cmd.CMDS_Names[cmd.NAME] = cmd

    async def Parse(self, strCmd, ctx):
        segs = re.split('\s+', strCmd)

        if len(segs) < 1:
            raise Exception('syntax error')

        if segs[0] not in Cmd.CMDS_Names:
            raise Exception('"%s" unknown command' % segs[0])

        await Cmd.CMDS_Names[segs[0]](segs[1:]).run(ctx)


async def handle():
    ctx = Ctx()
    cmdParser = Cmd()

    while True and not ctx.finish:
        user_input = await prompt_async(
            '> ',
            history=FileHistory('history.txt'),
            auto_suggest=AutoSuggestFromHistory(),
            completer=CommandCompleter,
            patch_stdout=True
        )

        #	message = click.edit()

        user_input = user_input.strip()
        if len(user_input) > 0:
            try:
                await cmdParser.Parse(user_input, ctx)
            except Exception as e:
                logging.exception('Error')
                click.secho("Error: %s" % e.args[0], fg='red')
    #		print(traceback.format_exc())


logging.basicConfig(filename="client.log", level=logging.DEBUG)

zmq.asyncio.install()
asyncio.get_event_loop().run_until_complete(asyncio.gather(
    handle()
))
asyncio.get_event_loop().close()
