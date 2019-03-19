import pytest
import sys
import json

from os.path import abspath, join, isdir, exists
from shutil import rmtree
from string import Template
from datetime import datetime, timedelta

from qcg.appscheduler.service import QCGPMService
from qcg.appscheduler.joblist import Job, JobExecution, JobResources, ResourceSize
from qcg.appscheduler.errors import IllegalJobDescription

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
    assert all((isdir(abspath(tmpdir.join('date.sandbox'))),
               exists(join(abspath(tmpdir.join('date.sandbox')), 'date.out')),
               exists(join(abspath(tmpdir.join('date.sandbox')), 'date.err'))))

    with pytest.raises(ValueError):
        check_job_status_in_json([jobName + 'xxx'], workdir=str(tmpdir), dest_state='SUCCEED')

    rmtree(str(tmpdir))


def test_local_error_duplicate_name_job(tmpdir):
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
        ),
        Job(jobName,
            JobExecution(
                '/usr/bin/sleep',
                wd=abspath(tmpdir.join('sleep.sandbox') ),
                stdout='sleep.out',
                stderr='sleep.err'
            ),
            JobResources(numCores=ResourceSize(1))
            )
    ] ]
    reqs = [ { 'request': 'submit', 'jobs': jobs },
            { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    sys.argv = [ 'QCG-PilotJob', '--file', '--file-path', str(file_path), '--nodes', '2', '--wd', str(tmpdir),
                 '--report-format', 'json']
    QCGPMService().start()

    # no job should be executed due to the failed submit request with non-unique jobs inside
    assert not isdir(abspath(tmpdir.join('date.sandbox')))
    assert not isdir(abspath(tmpdir.join('sleep.sandbox')))

    rmtree(str(tmpdir))


def test_local_error_duplicate_name_job_separate_reqs(tmpdir):
    file_path = tmpdir.join('jobs.json')

    print('tmpdir: {}'.format(str(tmpdir)))

    jobName = 'mdate'
    jobs1 = [job.toDict() for job in [
        Job(jobName,
            JobExecution(
                '/usr/bin/date',
                wd = abspath(tmpdir.join('date.sandbox')),
                stdout = 'date.out',
                stderr = 'date.err'
            ),
            JobResources( numCores=ResourceSize(1) )
        ) ] ]
    jobs2 = [job.toDict() for job in [
        Job(jobName,
            JobExecution(
                '/usr/bin/sleep',
                wd=abspath(tmpdir.join('sleep.sandbox') ),
                stdout='sleep.out',
                stderr='sleep.err'
            ),
            JobResources(numCores=ResourceSize(1))
            )
    ] ]
    reqs = [ { 'request': 'submit', 'jobs': jobs1 },
             { 'request': 'submit', 'jobs': jobs2 },
             { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    sys.argv = [ 'QCG-PilotJob', '--file', '--file-path', str(file_path), '--nodes', '2', '--wd', str(tmpdir),
                 '--report-format', 'json']
    QCGPMService().start()

    # the first job (date) should execute
    check_job_status_in_json([ jobName ], workdir=str(tmpdir), dest_state='SUCCEED')
    assert all((isdir(abspath(tmpdir.join('date.sandbox'))),
               exists(join(abspath(tmpdir.join('date.sandbox')), 'date.out')),
               exists(join(abspath(tmpdir.join('date.sandbox')), 'date.err'))))

    # the second job (sleep) due to the name clash should not execute
    assert not isdir(abspath(tmpdir.join('sleep.sandbox')))

    rmtree(str(tmpdir))


def test_local_error_job_desc():
    # missing job execution
    with pytest.raises(IllegalJobDescription):
        Job('error_job',
            JobExecution(
                None,
                stdout = 'date.out',
                stderr = 'date.err'
            ),
            JobResources( numCores=ResourceSize(1) )
        )

    # wrong format of arguments
    with pytest.raises(IllegalJobDescription):
        Job('error_job',
            JobExecution(
                '/usr/bin/date',
                args = 'this should be a list',
                stdout = 'date.out',
                stderr = 'date.err'
            ),
            JobResources( numCores=ResourceSize(1) )
        )

    # wrong format of environment
    with pytest.raises(IllegalJobDescription):
        Job('error_job',
            JobExecution(
                '/usr/bin/date',
                args = ['arg1'],
                env = [ 'this shuld be a dict'],
                stdout = 'date.out',
                stderr = 'date.err'
            ),
            JobResources( numCores=ResourceSize(1) )
        )

    # missing execution definition
    with pytest.raises(IllegalJobDescription):
        Job('error_job',
            None,
            JobResources( numCores=ResourceSize(1) )
        )

    # missing resources definition
    with pytest.raises(IllegalJobDescription):
        Job('error_job',
            JobExecution(
                '/usr/bin/date',
                args = ['arg1'],
                env = [ 'this shuld be a dict'],
                stdout = 'date.out',
                stderr = 'date.err'
            ),
            None
        )


def test_local_simple_iter_job(tmpdir):
    file_path = tmpdir.join('jobs.json')

    print('tmpdir: {}'.format(str(tmpdir)))

    jobName = "echo-iter_${it}"
    nits = 10
    jobs = [
        {
            "name": jobName,
            "iterate": [0, nits],
            "execution": {
                "exec": "/bin/echo",
                "args": ["iteration ${it}"],
                "wd": abspath(tmpdir.join(jobName)),
                "stdout": "echo-iter.stdout",
                "stderr": "echo-iter.stderr"
            },
            "resources": {
                "numCores": {
                    "exact": 1,
                }
            }
        }
    ]
    reqs = [ { 'request': 'submit', 'jobs': jobs },
            { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    sys.argv = [ 'QCG-PilotJob', '--file', '--file-path', str(file_path), '--nodes', '2', '--wd', str(tmpdir),
                 '--report-format', 'json']
    QCGPMService().start()

    check_job_status_in_json([Template(jobName).substitute(it=i) for i in range(0, nits)], workdir=str(tmpdir), dest_state='SUCCEED')

    for i in range(0, nits):
        jname = Template(jobName).substitute(it=i)

        assert all((isdir(abspath(tmpdir.join(jname))),
                   exists(join(abspath(tmpdir.join(jname)), 'echo-iter.stdout')),
                   exists(join(abspath(tmpdir.join(jname)), 'echo-iter.stderr'))))

        with open(join(abspath(tmpdir.join(jname)), 'echo-iter.stdout'), 'r') as f:
            assert f.read().strip() == Template("iteration ${it}").substitute(it=i)

    rmtree(str(tmpdir))


def test_local_iter_scheduling_job(tmpdir):
    file_path = tmpdir.join('jobs.json')

    print('tmpdir: {}'.format(str(tmpdir)))

    jobName = "sleep-iter_${it}"
    nits = 4
    jobSleepTime = 2
    jobCores = 2
    availCores = 4
    rounds = nits * jobCores / availCores
    totalExecTime = rounds * jobSleepTime
    jobs = [
        {
            "name": jobName,
            "iterate": [0, nits],
            "execution": {
                "exec": "/bin/sleep",
                "args": ["{}s".format(str(jobSleepTime))],
                "wd": abspath(tmpdir.join(jobName)),
                "stdout": "sleep-iter.stdout",
                "stderr": "sleep-iter.stderr"
            },
            "resources": {
                "numCores": {
                    "exact": jobCores,
                }
            }
        }
    ]
    reqs = [ { 'request': 'submit', 'jobs': jobs },
            { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    sys.argv = [ 'QCG-PilotJob', '--file', '--file-path', str(file_path), '--nodes', str(availCores), '--wd', str(tmpdir),
                 '--report-format', 'json']
    QCGPMService().start()

    check_job_status_in_json([Template(jobName).substitute(it=i) for i in range(0, nits)], workdir=str(tmpdir), dest_state='SUCCEED')

    for i in range(0, nits):
        jname = Template(jobName).substitute(it=i)

        assert all((isdir(abspath(tmpdir.join(jname))),
                   exists(join(abspath(tmpdir.join(jname)), 'sleep-iter.stdout')),
                   exists(join(abspath(tmpdir.join(jname)), 'sleep-iter.stderr'))))

    with open(join(str(tmpdir), 'jobs.report'), 'r') as f:
        job_stats = [json.loads(line) for line in f.readlines() ]

    assert len(job_stats) == nits

    min_start, max_finish = None, None

    for i in range(0, nits):
        job = job_stats[i]

        print('readed job stats: {}'.format(str(job)))
        t = datetime.strptime(job['runtime']['rtime'], "%H:%M:%S.%f")
        rtime = timedelta(hours=t.hour, minutes=t.minute, seconds=t.second, microseconds=t.microsecond)

        assert all((rtime.total_seconds() > jobSleepTime, rtime.total_seconds() < jobSleepTime + 0.5)), \
            "job {} runtime exceeded assumed value {}s vs max {}s".format(i, rtime.total_seconds(), jobSleepTime + 0.5)

        # find start executing time
        exec_state = list(filter(lambda st_en: st_en['state'] == 'EXECUTING', job['history']))
        assert len(exec_state) == 1

        finish_state = list(filter(lambda st_en: st_en['state'] == 'SUCCEED', job['history']))
        assert len(finish_state) == 1

        start_time = datetime.strptime(exec_state[0]['date'], '%Y-%m-%dT%H:%M:%S.%f')
        finish_time = datetime.strptime(finish_state[0]['date'], '%Y-%m-%dT%H:%M:%S.%f')

        if not min_start or start_time < min_start:
            min_start = start_time

        if not max_finish or finish_time > max_finish:
            max_finish = finish_time

    assert all((min_start, finish_time))

    # check if duration from executing first job till the end of last job is about 2 rounds, each with jobSleepTime
    scenario_duration = finish_time - min_start
    assert all((scenario_duration.total_seconds() > totalExecTime,
                scenario_duration.total_seconds() < totalExecTime + 1)), \
            "scenario duration runtime exceeded assumed value {}s vs max {}s".format(scenario_duration.total_seconds(),
                                                                                     totalExecTime + 1)

    rmtree(str(tmpdir))


def test_local_iter_scheduling_job_large(tmpdir):
    file_path = tmpdir.join('jobs.json')

    print('tmpdir: {}'.format(str(tmpdir)))

    jobName = "sleep-iter_${it}"
    nits = 100
    jobSleepTime = 2
    jobCores = 2
    availCores = 40
    rounds = nits * jobCores / availCores
    totalExecTime = rounds * jobSleepTime
    jobs = [
        {
            "name": jobName,
            "iterate": [0, nits],
            "execution": {
                "exec": "/bin/sleep",
                "args": ["{}s".format(str(jobSleepTime))],
                "wd": abspath(tmpdir.join(jobName)),
                "stdout": "sleep-iter.stdout",
                "stderr": "sleep-iter.stderr"
            },
            "resources": {
                "numCores": {
                    "exact": jobCores,
                }
            }
        }
    ]
    reqs = [ { 'request': 'submit', 'jobs': jobs },
            { 'request': 'control', 'command': 'finishAfterAllTasksDone' } ]
    save_reqs_to_file(reqs, file_path)
    print('jobs saved to file_path: {}'.format(str(file_path)))

    sys.argv = [ 'QCG-PilotJob', '--file', '--file-path', str(file_path), '--nodes', str(availCores), '--wd', str(tmpdir),
                 '--report-format', 'json']
    QCGPMService().start()

    check_job_status_in_json([Template(jobName).substitute(it=i) for i in range(0, nits)], workdir=str(tmpdir), dest_state='SUCCEED')

    for i in range(0, nits):
        jname = Template(jobName).substitute(it=i)

        assert all((isdir(abspath(tmpdir.join(jname))),
                   exists(join(abspath(tmpdir.join(jname)), 'sleep-iter.stdout')),
                   exists(join(abspath(tmpdir.join(jname)), 'sleep-iter.stderr'))))

    with open(join(str(tmpdir), 'jobs.report'), 'r') as f:
        job_stats = [json.loads(line) for line in f.readlines() ]

    assert len(job_stats) == nits

    min_start, max_finish = None, None

    for i in range(0, nits):
        job = job_stats[i]

        print('readed job stats: {}'.format(str(job)))
        t = datetime.strptime(job['runtime']['rtime'], "%H:%M:%S.%f")
        rtime = timedelta(hours=t.hour, minutes=t.minute, seconds=t.second, microseconds=t.microsecond)

        assert all((rtime.total_seconds() > jobSleepTime, rtime.total_seconds() < jobSleepTime + 1)), \
            "job {} runtime exceeded assumed value {}s vs max {}s".format(i, rtime.total_seconds(), jobSleepTime + 1)

        # find start executing time
        exec_state = list(filter(lambda st_en: st_en['state'] == 'EXECUTING', job['history']))
        assert len(exec_state) == 1

        finish_state = list(filter(lambda st_en: st_en['state'] == 'SUCCEED', job['history']))
        assert len(finish_state) == 1

        start_time = datetime.strptime(exec_state[0]['date'], '%Y-%m-%dT%H:%M:%S.%f')
        finish_time = datetime.strptime(finish_state[0]['date'], '%Y-%m-%dT%H:%M:%S.%f')

        if not min_start or start_time < min_start:
            min_start = start_time

        if not max_finish or finish_time > max_finish:
            max_finish = finish_time

    assert all((min_start, finish_time))

    # check if duration from executing first job till the end of last job is about 2 rounds, each with jobSleepTime
    scenario_duration = finish_time - min_start
    assert all((scenario_duration.total_seconds() > totalExecTime,
                scenario_duration.total_seconds() < totalExecTime + 3)), \
            "scenario duration runtime exceeded assumed value {}s vs max {}s".format(scenario_duration.total_seconds(),
                                                                                     totalExecTime + 3)

    rmtree(str(tmpdir))