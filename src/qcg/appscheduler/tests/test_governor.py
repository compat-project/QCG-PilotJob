import sys
import os
import time
from shutil import rmtree

from qcg.appscheduler.service import QCGPMService
from qcg.appscheduler.tests.utils import save_reqs_to_file, check_service_log_string, fork_manager, send_request_valid



def test_manager_id(tmpdir):
    file_path = tmpdir.join('jobs.json')

    print('tmpdir: {}'.format(str(tmpdir)))

    reqs = [ { 'request': 'resourcesInfo' },
            { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    manager_id = 'custom_manager_id'

    sys.argv = [ 'QCG-PilotJob', '--log', 'debug', '--file', '--file-path', str(file_path), '--nodes', '2',
                 '--wd', str(tmpdir), '--report-format', 'json', '--id', manager_id ]
    QCGPMService().start()

    assert(check_service_log_string('service {} version'.format(manager_id), tmpdir)), 'not valid manager id'
    rmtree(str(tmpdir))


def test_manager_tags(tmpdir):
    file_path = tmpdir.join('jobs.json')

    print('tmpdir: {}'.format(str(tmpdir)))

    reqs = [ { 'request': 'resourcesInfo' },
            { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    manager_id = 'custom_taged_manager'
    manager_tags = [ 'custom', 'first' , 'manager' ]

 #   sys.argv = ['QCG-PilotJob', '--log', 'debug', '--file', '--file-path', str(file_path), '--nodes', '2',
 #               '--wd', str(tmpdir), '--report-format', 'json', '--parent', 'localhost:7777']

    sys.argv = [ 'QCG-PilotJob', '--log', 'debug', '--file', '--file-path', str(file_path), '--nodes', '2',
                 '--wd', str(tmpdir), '--report-format', 'json', '--id', manager_id, '--tags', ','.join(manager_tags) ]
    QCGPMService().start()

    assert(check_service_log_string('service {} version'.format(manager_id), tmpdir)), 'not valid manager id'
    assert(check_service_log_string('(with tags {})'.format(','.join([manager_id, *manager_tags])), tmpdir)),\
        'not valid manager tags'
    rmtree(str(tmpdir))


def test_register_manager(tmpdir):
    governor_dir = tmpdir.join('governor')
    direct_dir = tmpdir.join('direct1')

    os.makedirs(str(governor_dir))
    os.makedirs(str(direct_dir))

    print('governor work dir: {}'.format(str(governor_dir)))
    print('direct work dir: {}'.format(str(direct_dir)))

    governor_id = 'm-governor'

    governor_args = [ '--log', 'debug', '--wd', str(governor_dir), '--report-format', 'json', '--id', governor_id,
                      '--governor' ]
    governor_process, governor_address = fork_manager(governor_args)

    reqs = [ {'request': 'resourcesInfo'},
             {'request': 'control', 'command': 'finishAfterAllTasksDone'} ]
    reqs_path = direct_dir.join('jobs.json')
    save_reqs_to_file(reqs, reqs_path)
    print('jobs saved to file_path: {}'.format(str(reqs_path)))

    direct_id = 'm-direct-1'
    manager_tags = ['custom', 'first', 'manager']

    sys.argv = ['QCG-PilotJob', '--log', 'debug', '--file', '--file-path', str(reqs_path), '--nodes', '2',
                '--wd', str(direct_dir), '--report-format', 'json', '--id', direct_id, '--tags', ','.join(manager_tags),
                '--parent', governor_address ]

    QCGPMService().start()

    send_request_valid(governor_address, { 'request': 'finish' })
    governor_process.join()
    governor_process.terminate()


def test_register_manager_resources_single(tmpdir):
    governor_dir = tmpdir.join('governor')
    direct_dir = tmpdir.join('direct1')

    os.makedirs(str(governor_dir))
    os.makedirs(str(direct_dir))

    governor_process = direct_process = None

    try:
        governor_id = 'm-governor'
        governor_process, governor_address = fork_manager([ '--log', 'debug', '--wd', str(governor_dir), '--report-format',
            'json', '--id', governor_id, '--governor' ])

        direct_id = 'm-direct-1'
        direct_tags = ['custom', 'first', 'manager']
        direct_nodes = 2
        direct_cores = 4
        nodes_conf = ','.join(['n{}:{}'.format(idx, direct_cores) for idx in range(direct_nodes)])
        direct_process, direct_address = fork_manager(['--log', 'debug',
            '--nodes', nodes_conf, '--wd', str(direct_dir), '--report-format', 'json',
            '--id', direct_id, '--tags', ','.join(direct_tags), '--parent', governor_address ])

        # get managers time to communicate and exchange info about resources
        time.sleep(2)

        resources_reply = send_request_valid(governor_address, { 'request': 'resourcesInfo' })
        assert all((resources_reply['code'] == 0, 'data' in resources_reply))
        assert all((resources_reply['data'].get('totalNodes', 0) == direct_nodes,
                    resources_reply['data'].get('totalCores', 0) == direct_nodes * direct_cores,
                    resources_reply['data'].get('usedCores', -1) == 0,
                    resources_reply['data'].get('freeCores', 0) == direct_nodes * direct_cores)), str(resources_reply)

        send_request_valid(governor_address, { 'request': 'finish' })
        governor_process.join()

        send_request_valid(direct_address, { 'request': 'finish' })
        direct_process.join()
    finally:
        if governor_process:
            governor_process.terminate()

        if direct_process:
            direct_process.terminate()


def test_register_manager_resources_many(tmpdir):
    governor_dir = tmpdir.join('governor')
    direct_dir = tmpdir.join('direct')

    instances = 4

    os.makedirs(str(governor_dir))
    for i in range(instances):
        os.makedirs('{}_{}'.format(str(direct_dir), i))

    governor_process = None
    direct_process = [None] * instances
    direct_address = [None] * instances

    try:
        governor_id = 'm-governor'
        governor_process, governor_address = fork_manager([ '--log', 'debug', '--wd', str(governor_dir), '--report-format',
            'json', '--id', governor_id, '--governor' ])

        direct_id = 'm-direct'
        direct_tags = ['custom', 'first', 'manager']

        direct_nodes = [0] * instances
        direct_cores = [0] * instances

        for i in range(instances):
            direct_nodes[i] = i + 1
            direct_cores[i] = (i + 1) * 2
            nodes_conf = ','.join(['n{}:{}'.format(idx, direct_cores[i]) for idx in range(direct_nodes[i])])
            direct_process[i], direct_address[i] = fork_manager(['--log', 'debug',
                '--nodes', nodes_conf, '--wd', str(direct_dir), '--report-format', 'json',
                '--id', '{}.{}'.format(direct_id, i), '--tags', ','.join(direct_tags), '--parent', governor_address ])

        # get managers time to communicate and exchange info about resources
        time.sleep(2)

        resources_reply = send_request_valid(governor_address, { 'request': 'resourcesInfo' })
        print('got total resource reply: {}'.format(str(resources_reply)))
        assert all((resources_reply['code'] == 0, 'data' in resources_reply))
        assert all((resources_reply['data'].get('totalNodes', 0) == sum(direct_nodes),
                    resources_reply['data'].get('totalCores', 0) == sum([direct_nodes[i] * direct_cores[i] for i in range(instances)]),
                    resources_reply['data'].get('usedCores', -1) == 0,
                    resources_reply['data'].get('freeCores', 0) == sum([direct_nodes[i] * direct_cores[i] for i in range(instances)]))),\
            str(resources_reply)

        send_request_valid(governor_address, { 'request': 'finish' })
        governor_process.join()

        for address in direct_address:
            send_request_valid(address, { 'request': 'finish' })

        for proc in direct_process:
            proc.join()
    finally:
        if governor_process:
            governor_process.terminate()

        for proc in direct_process:
            if proc:
                proc.terminate()


