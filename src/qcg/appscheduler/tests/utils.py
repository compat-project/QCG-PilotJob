import os
import json
import re
import multiprocessing as mp
import queue
import zmq
import time
from datetime import datetime

from json import JSONDecodeError
from qcg.appscheduler.service import QCGPMServiceProcess
from qcg.appscheduler.joblist import JobState
from qcg.appscheduler.utils.auxdir import find_single_aux_dir


def save_jobs_to_file(jobs, file_path):
    if os.path.exists(file_path):
        os.remove(file_path)

    with open(file_path, 'w') as f:
        f.write(json.dumps([ job.toDict() for job in jobs ], indent=2))


def save_reqs_to_file(reqs, file_path):
    if os.path.exists(file_path):
        os.remove(file_path)

    with open(file_path, 'w') as f:
        f.write(json.dumps(reqs, indent=2))


def check_job_status_in_json(jobs, workdir='.', dest_state='SUCCEED'):
    to_check = set(jobs)

    report_path = os.path.join(find_single_aux_dir(workdir), 'jobs.report')
    with open(report_path, 'r') as report_f:
        for line, entry in enumerate(report_f, 1):
            try:
                job_entry = json.loads(entry)
            except JSONDecodeError as e:
                raise ValueError('wrong jobs.report {} file format: error in {} line: {}'.format(report_path, line, str(e)))

            for attr in [ 'name', 'state' ]:
                if not attr in job_entry:
                    raise ValueError('wrong jobs.report {} file format: missing \'{}\' attribute'.format(report_path, attr))

            jName = job_entry['name']
            state = job_entry['state']

            if jName in to_check:
                msg = job_entry.get('message', None)
                if state != dest_state:
                    raise ValueError('job\'s {} incorrect status \'{}\' vs expected \'{}\' {}'.format(jName, state, dest_state,
                        '(' + msg  + ')' if msg else ''))

                to_check.remove(jName)

            if len(to_check) == 0:
                break

    if len(to_check) > 0:
        raise ValueError('missing {} jobs in report'.format(','.join(to_check)))


def check_service_log_string(resubstr, workdir='.'):
    service_log_file = os.path.join(find_single_aux_dir(workdir), 'service.log')

    regex = re.compile(resubstr)

    with open(service_log_file, 'r') as log_f:
        for line in log_f:
            if regex.search(line):
                return True

    return False


def fork_manager(manager_args):
    if not mp.get_context():
        mp.set_start_method('fork')

    qcgpm_queue = mp.Queue()
    qcgpm_process = QCGPMServiceProcess(manager_args, qcgpm_queue)
    qcgpm_process.start()

    try:
        qcgpm_conf = qcgpm_queue.get(block=True, timeout=10)
    except queue.Empty:
        raise Exception('Service not started - timeout')
    except Exception as e:
        raise Exception('Service not started: {}'.format(str(e)))

    if not qcgpm_conf.get('zmq_addresses', None):
        raise Exception('Missing QCGPM network interface address')

    return qcgpm_process, qcgpm_conf['zmq_addresses'][0]


def send_request_valid(address, req_data):
    ctx = zmq.Context.instance()
    out_socket = ctx.socket(zmq.REQ)
    out_socket.connect(address)

    try:
        out_socket.send_json(req_data)
        msg = out_socket.recv_json()
        if not msg['code'] == 0:
            raise Exception('request failed: {}'.format(msg.get('message', '')))

        return msg
    finally:
        if out_socket:
            out_socket.close()


def wait_for_job_finish_success(manager_address, jobnames, timeout_secs=0):
    start_time = datetime.now()

    while len(jobnames):
        current_jobnames = []

        status_reply = send_request_valid(manager_address, {'request': 'jobStatus', 'jobNames': jobnames})
        assert all((status_reply['code'] == 0, 'data' in status_reply))
        assert 'jobs' in status_reply['data']
        assert len(status_reply['data']['jobs']) > 0

        for jname in jobnames:
            assert jname in status_reply['data']['jobs']
            jstatus = status_reply['data']['jobs'][jname]
            assert all((jstatus['status'] == 0, jstatus['data']['jobName'] == jname))

            jstate = JobState[jstatus['data']['status']]
            if jstate.isFinished():
                assert jstate == JobState.SUCCEED
            else:
                current_jobnames.append(jname)

        time.sleep(0.5)
        if timeout_secs and (datetime.now() - start_time).total_seconds() > timeout_secs:
            raise Exception('Timeout occurred')

        jobnames = current_jobnames
