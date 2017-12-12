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

CommandCompleter = WordCompleter( [ 'jobs', 'status', 'jinfo', 'load', 'edit', 'jcancel', 'connect', 'exit' ], ignore_case = True )


class Ctx:

	def __init__(self):
		self.zmqCtx = Context.instance()
		self.zmqSock = None
		self.connected = False
		self.finish = False

	async def waitForFinish(self):
		while not self.finish:
			asyncio.sleep(1)


class CmdJobs:
	NAME = 'jobs'

	def __init__(self, args):
		if args is not None and len(args) > 0:
			raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, self.help_str()))

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

		if 'names' not in d['data']:
			raise Exception('invalid reply from the service')

		status = []
		for jName in d['data']['names']:
			jStatus = None

			# get job status
			await ctx.zmqSock.send(str.encode(json.dumps({
				"request": "jobStatus",
				"jobName": jName
			})))
			jReply = bytes.decode(await ctx.zmqSock.recv())
			jD = json.loads(jReply)
			if 'code' in jD and jD['code'] == 0 and 'data' in jD and 'status' in jD['data']:
				jStatus = jD['data']['status']
			
			if jStatus is not None:
				status.append("%s (%s)" % (jName, jStatus))
			else:
				status.append('%s' % jName)

		statusStr = '\n'.join(status)

		click.secho(statusStr, fg='green')


	def help_str(self):
		return '''%s syntax:\n\t%s''' % (self.NAME, self.NAME)


class CmdStatus:
	NAME = 'status'

	def __init__(self, args):
		if args is not None and len(args) > 0:
			raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, self.help_str()))

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


	def help_str(self):
		return '''%s syntax:\n\t%s''' % (self.NAME, self.NAME)


class CmdJinfo:
	NAME = 'jinfo'

	def __init__(self, args):
		if args is None or len(args) != 1:
			raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, self.help_str()))

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
		status = "%s" % ( data['status'] )

		color = 'blue'
		if status in [ 'FAILED', 'CANCELED', 'OMITTED' ]:
			color = 'red'
		elif status in [ 'SUCCEED' ]:
			color = 'green'

		click.secho(status, fg=color)


	def help_str(self):
		return '''%s syntax:\n\t%s {job name}''' % (self.NAME, self.NAME)


class CmdLoad:
	NAME = 'load'

	def __init__(self, args):
		if args is None or len(args) != 1:
			raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, self.help_str()))

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


	def help_str(self):
		return '''%s syntax:\n\t%s {requests file}''' % (self.NAME, self.NAME)


class CmdJcancel:
	NAME = 'jcancel'

	def __init__(self, args):
		if args is None or len(args) != 1:
			raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, self.help_str()))

		self.jName = args[0]

	async def run(self, ctx):
		print("canceling job '%s' ..." % self.jName)

	def help_str(self):
		return '''%s syntax:\n\t%s {job name}''' % (self.NAME, self.NAME)


class CmdConnect:
	NAME = 'connect'

	def __init__(self, args):
		if args is None or len(args) != 1:
			self.address = "https://127.0.0.1:5555"
		else:
			self.address = args[0]

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


	def help_str(self):
		return '''%s syntax:\n\t%s {address}''' % (self.NAME, self.NAME)


class CmdExit:
	NAME = 'exit'

	def __init__(self, args):
		if args is not None and len(args) > 0:
			raise Exception('"%s" wrong syntax\n\n%s' % (self.NAME, self.help_str()))

	async def run(self, ctx):
		click.secho("exiting ...", fg='green')

		ctx.zmqSock.close()
		ctx.connected = False
		ctx.finish = True

	def help_str(self):
		return '''%s syntax:\n\t%s''' % (self.NAME, self.NAME)


class Cmd:
	__CMDS = [
			CmdJobs,
			CmdStatus,
			CmdJinfo,
			CmdLoad,
			CmdJcancel,
			CmdConnect,
			CmdExit
	]

	__CMDS_Names = { }

	def __init__(self):
		__CMDS_Names = { }

		for cmd in Cmd.__CMDS:
			Cmd.__CMDS_Names[cmd.NAME] = cmd

	async def Parse(self, strCmd, ctx):
		segs = re.split('\s+', strCmd)

		if len(segs) < 1:
			raise Exception('syntax error')

		if segs[0] not in Cmd.__CMDS_Names:
			raise Exception('"%s" unknown command' % segs[0])

		await Cmd.__CMDS_Names[segs[0]](segs[1:]).run(ctx)


async def handle():
	ctx = Ctx()
	cmdParser = Cmd()

	while True and not ctx.finish:
		user_input = await prompt_async(
				'> ',
				history = FileHistory('history.txt'),
				auto_suggest = AutoSuggestFromHistory(),
				completer = CommandCompleter,
				patch_stdout = True
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


