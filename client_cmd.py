import click
import python
import logging
import re
import traceback
import json
import zmq

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
@click.option('-d', '--dir', envvar='QCG_PJM_DIR', help="path to the QCG-PJM working directory")
@click.option('-a', '--address', envvar='QCG_PJM_ADDRESS', help="address of the QCG-PJM network interface")
@click.option('-d', '--debug', envvar='QCG_CLIENT_DEBUG', is_flag=True, default=False, help="enable debugging - all messages will be written to the local qcgclient.log file")
@click.pass_context
def qcgpjm(ctx, dir, address, debug):
    """QCG PJM command line client."""
    try:
        setup_logging(debug)

        if address:
            pjm_address = get_address_from_arg(address)
        elif dir:
            pjm_address = get_address_from_directory(dir)
        else:
            pjm_address = get_address_from_directory('.')

        if not pjm_address:
            raise Exception('unable to find QCG PJM network interface address (use \'-d\' or \'-a\' argument)')

        ctx.address = pjm_address

        logging.debug('qcg pjm network interface address: {}'.format(str(ctx.address)))

        ctx.zmqCtx = zmq.Context.instance()
        ctx.zmqSock = ctx.zmqCtx.socket(zmq.REQ)
        ctx.zmqSock.connect(ctx.address)
    except Exception as e:
        click.echo('error: {}'.format(str(e)), err=True)
        logging.error(traceback.format_exc())


@qcgpjm.command()
@click.pass_obj
def list(ctx):
    """Show service status."""
    try:
        path = 'jobs/'

        ctx.zmqSock.send(str.encode(json.dumps({
            "request": "status"
        })))

        logging.debug('\'status\' request send - waiting for reponse')

        reply = bytes.decode(ctx.zmqSock.reccv())

        logging.debug('got reply: {}'.format(str(reply)))

        d = json.loads(reply)

        print('current status: {}'.format(str(reply)))
    except Exception as e:
        click.echo('error: {}'.format(str(e)), err=True)
        logging.error(traceback.format_exc())



def setup_logging(debug):
    level=logging.DEBUG if debug else logging.WARNING
    logging.basicConfig(level=level,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename='qcgclient.log',
                        filemode='w')


if __name__ == '__main__':
    qcgpjm()
