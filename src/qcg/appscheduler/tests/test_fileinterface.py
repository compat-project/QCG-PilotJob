import pytest
import sys

from os.path import abspath, join
from shutil import rmtree

from qcg.appscheduler.service import QCGPMService
from qcg.appscheduler.joblist import Job, JobExecution, JobResources, ResourceSize

from qcg.appscheduler.tests.utils import save_reqs_to_file, check_job_status_in_json


def test_local_simple_job(tmpdir):
    file_path = tmpdir.join('jobs.json')

    print('tmpdir: {}'.format(str(tmpdir)))

    jobName = 'mdate'
    jobs = [job.toDict() for job in [
        Job(jobName,
            JobExecution(
                '/usr/bin/date',
                wd = abspath(tmpdir.join('date.sandbox')),
                stdout = 'date.out',
                stderr = 'date.err'
            ),
            JobResources( numCores=ResourceSize(1) )
        )
    ] ]
    reqs = [ { 'request': 'submit', 'jobs': jobs },
            { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    sys.argv = [ 'QCG-PilotJob', '--file', '--file-path', str(file_path), '--nodes', '2', '--wd', str(tmpdir),
                 '--report-format', 'json']
    QCGPMService().start()

    check_job_status_in_json([ jobName ], workdir=str(tmpdir), dest_state='SUCCEED')

    with pytest.raises(ValueError):
        check_job_status_in_json([jobName + 'xxx'], workdir=str(tmpdir), dest_state='SUCCEED')

    rmtree(str(tmpdir))


def test_local_error_job(tmpdir):
    file_path = tmpdir.join('jobs.json')

    print('tmpdir: {}'.format(str(tmpdir)))

    jobName = 'mdate'
    jobs = [job.toDict() for job in [
        Job(jobName,
            JobExecution(
                '/usr/bin/date',
                wd = abspath(tmpdir.join('date.sandbox')),
                stdout = 'date.out',
                stderr = 'date.err'
            ),
            JobResources( numCores=ResourceSize(1) )
        )
    ] ]
    reqs = [ { 'request': 'submit', 'jobs': jobs },
            { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    sys.argv = [ 'QCG-PilotJob', '--file', '--file-path', str(file_path), '--nodes', '2', '--wd', str(tmpdir),
                 '--report-format', 'json']
    QCGPMService().start()

    check_job_status_in_json([ jobName ], workdir=str(tmpdir), dest_state='SUCCEED')

    with pytest.raises(ValueError):
        check_job_status_in_json([jobName + 'xxx'], workdir=str(tmpdir), dest_state='SUCCEED')

    rmtree(str(tmpdir))


def test_local_iterative_job(tmpdir):
    file_path = tmpdir.join('jobs.json')

    print('tmpdir: {}'.format(str(tmpdir)))

    jobName = 'mdate'
    jobs = [job.toDict() for job in [
        Job(jobName,
            JobExecution(
                '/usr/bin/date',
                wd = abspath(tmpdir.join('date.sandbox')),
                stdout = 'date.out',
                stderr = 'date.err'
            ),
            JobResources( numCores=ResourceSize(1) )
        )
    ] ]
    reqs = [ { 'request': 'submit', 'jobs': jobs },
            { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    sys.argv = [ 'QCG-PilotJob', '--file', '--file-path', str(file_path), '--nodes', '2', '--wd', str(tmpdir),
                 '--report-format', 'json']
    QCGPMService().start()

    check_job_status_in_json([ jobName ], workdir=str(tmpdir), dest_state='SUCCEED')

    with pytest.raises(ValueError):
        check_job_status_in_json([jobName + 'xxx'], workdir=str(tmpdir), dest_state='SUCCEED')

    rmtree(str(tmpdir))