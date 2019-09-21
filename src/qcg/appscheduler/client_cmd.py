import click
import logging
import re
import traceback
import json
import zmq
import sys

from os.path import exists, join

DEFAULT_PROTO = 'tcp'
DEFAULT_PORT = '21000'


def get_address_from_directory(path):
    wdir = join(path, '.qcgpjm')
    if exists(wdir) and exists(join(wdir, 'address')):
        with open(join(wdir, 'address'), 'r') as f:
            return f.read()

    return None


def get_address_from_arg(address):
    result = address

    if not re.match('\w*://', result):
        # append default protocol
        result = "%s://%s" % (CmdConnect.DEFAULT_PROTO, result)

    if not re.match('.*:\d+', result):
        # append default port
        result = "%s:%s" % (result, CmdConnect.DEFAULT_PORT)

    return result



@click.group()
@click.option('-p', '--path', envvar='QCG_PJM_DIR', help="path to the QCG-PJM working directory")
@click.option('-a', '--address', envvar='QCG_PJM_ADDRESS', help="address of the QCG-PJM network interface")
@click.option('-d', '--debug', envvar='QCG_CLIENT_DEBUG', is_flag=True, default=False, help="enable debugging - all messages will be written to the local qcgclient.log file")
@click.pass_context
def qcgpjm(ctx, path, address, debug):
    """QCG PJM command line client."""
    try:
        setup_logging(debug)

        if address:
            pjm_address = get_address_from_arg(address)
        elif path:
            pjm_address = get_address_from_directory(path)
        else:
            pjm_address = get_address_from_directory('.')

        if not pjm_address:
            raise Exception('unable to find QCG PJM network interface address (use \'-p\' or \'-a\' argument)')

        ctx.ensure_object(dict)
        ctx.obj['address'] = pjm_address

        logging.debug('qcg pjm network interface address: {}'.format(str(ctx.obj['address'])))

        ctx.obj['zmqCtx'] = zmq.Context.instance()
        ctx.obj['zmqSock'] = ctx.obj['zmqCtx'].socket(zmq.REQ)
        ctx.obj['zmqSock'].setsockopt(zmq.LINGER, 0)
        ctx.obj['zmqSock'].connect(ctx.obj['address'])

        logging.debug('prepared context: {}'.format(str(ctx)))
    except Exception as e:
        click.echo('error: {}'.format(str(e)), err=True)
        logging.error(traceback.format_exc())
        sys.exit(1)


@qcgpjm.command()
@click.pass_context
def status(ctx):
    """Show service status."""
    try:
        logging.debug('got context: {}'.format(str(ctx)))

        path = 'jobs/'

        ctx.obj['zmqSock'].send(str.encode(json.dumps({
            "request": "status"
        })))

        logging.debug('\'status\' request send - waiting for reponse')

        poller = zmq.Poller()
        poller.register(ctx.obj['zmqSock'], zmq.POLLIN)
        if poller.poll(5*1000): # 10s timeout in milliseconds
            d = ctx.obj['zmqSock'].recv_json()
        else:
            raise Exception('Timeout processing status request')

        logging.debug('got reply: {}'.format(str(d)))

        if d['code'] != 0:
            raise Exception('Status request failed: {}'.format(d.get('message', 'error')))

        status = d.get('data', {})
        for secname, secdata in status.items():
            print('[{}]'.format(secname))
            for key, value in secdata.items():
                print('{}={}'.format(key, value))
    except Exception as e:
        click.echo('error: {}'.format(str(e)), err=True)
        logging.error(traceback.format_exc())



def setup_logging(debug):
    level=logging.DEBUG if debug else logging.WARNING

    if exists('.qcgpjm'):
        logfile = '.qcgpjm/qcgpjmclient.log'
    else:
        logfile = 'qcgpjmclient.log'

    logging.basicConfig(level=level,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename=logfile,
                        filemode='w')


if __name__ == '__main__':
    qcgpjm(obj={})
