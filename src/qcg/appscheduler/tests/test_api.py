import pytest

from os import stat
from os.path import join, exists

from qcg.appscheduler.api.job import Jobs, MAX_ITERATIONS
from qcg.appscheduler.api.errors import InvalidJobDescriptionError, JobNotDefinedError, FileError


def test_api_jobs_smpl_errors():
    jobs = Jobs()

    # missing job name
#    with pytest.raises(InvalidJobDescriptionError, match=r".*Missing job name.*"):
#        jobs.add()

    # unknown attribute
    with pytest.raises(InvalidJobDescriptionError, match=r".*Unknown attribute.*"):
        jobs.add(name='j1', unknownAttribute=True)

    # invalid attribute type
    with pytest.raises(InvalidJobDescriptionError, match=r".*Invalid attribute exec type.*"):
        jobs.add(name='j1', exec=2)
    with pytest.raises(InvalidJobDescriptionError, match=r".*Invalid attribute args type.*"):
        jobs.add(name='j1', args={'1': 'one'})

    # both exec and script
    with pytest.raises(InvalidJobDescriptionError, match=r".*Both 'exec' and 'script' defined.*"):
        jobs.add(name='j1', exec='/bin/date', script='date')

    # no exec nor script
    with pytest.raises(InvalidJobDescriptionError, match=r".*No 'exec' nor 'script' defined.*"):
        jobs.add(name='j1')

    # resources
    ## unknown resources attribute
    with pytest.raises(InvalidJobDescriptionError, match=r".*Unknown numCores attribute unknownAttr.*"):
        jobs.add(name='j1', exec='/bin/date', numCores={'unknownAttr': 1})
    ## wrong resources attribute type
    with pytest.raises(InvalidJobDescriptionError, match=r".*Invalid numCores attribute exact type.*"):
        jobs.add(name='j1', exec='/bin/date', numCores={'exact': '1'})
    with pytest.raises(InvalidJobDescriptionError, match=r".*Unknown numNodes attribute unknownAttr.*"):
        jobs.add(name='j1', exec='/bin/date', numNodes={'unknownAttr': 1})
    ## wrong resources attribute type
    with pytest.raises(InvalidJobDescriptionError, match=r".*Invalid numNodes attribute exact type.*"):
        jobs.add(name='j1', exec='/bin/date', numNodes={'exact': '1'})

    # iterate
    ## wrong type
    with pytest.raises(InvalidJobDescriptionError, match=r".*Invalid attribute iterate type.*"):
        jobs.add(name='j1', exec='/bin/date', iterate='some number')
    with pytest.raises(InvalidJobDescriptionError, match=r".*Invalid attribute iterate type.*"):
        jobs.add(name='j1', exec='/bin/date', iterate={'some invalid value '})
    with pytest.raises(InvalidJobDescriptionError, match=r".*Unknown iterate attribute unknown attribute.*"):
        jobs.add(name='j1', exec='/bin/date', iterate={'unknown attribute': 3})
    ## # of iterations outside of range
    with pytest.raises(InvalidJobDescriptionError, match=r".*Wrong number of iterations.*"):
        jobs.add(name='j1', exec='/bin/date', iterate=-1)
    with pytest.raises(InvalidJobDescriptionError, match=r".*Wrong number of iterations.*"):
        jobs.add(name='j1', exec='/bin/date', iterate={'start': 2, 'stop': 1})
    with pytest.raises(InvalidJobDescriptionError, match=r".*Wrong number of iterations.*"):
        jobs.add(name='j1', exec='/bin/date', iterate={'stop': -2})
    with pytest.raises(InvalidJobDescriptionError, match=r".*Wrong number of iterations.*"):
        jobs.add(name='j1', exec='/bin/date', iterate=MAX_ITERATIONS + 1)
    with pytest.raises(InvalidJobDescriptionError, match=r".*Wrong number of iterations.*"):
        jobs.add(name='j1', exec='/bin/date', iterate={'stop': MAX_ITERATIONS + 1})
    with pytest.raises(InvalidJobDescriptionError, match=r".*Wrong number of iterations.*"):
        jobs.add(name='j1', exec='/bin/date', iterate={'start': 2, 'stop': MAX_ITERATIONS + 3})
    with pytest.raises(InvalidJobDescriptionError, match=r".*Required attribute stop of element iterate not defined.*"):
        jobs.add(name='j1', exec='/bin/date', iterate={'start': 2})

    jobs.add(name='j1', exec='/bin/date')
    with pytest.raises(InvalidJobDescriptionError, match=r".*Job j1 already in list.*"):
        jobs.add(name='j1', exec='/bin/echo')
    jobs.remove('j1')
    assert len(jobs.jobs()) == 0

    with pytest.raises(JobNotDefinedError):
        jobs.remove('j1')


def test_api_jobs_smpl():
    jobs = Jobs()

    jobs.add(exec='/bin/date').add(exec='/bin/echo')

    assert all((len(jobs.jobNames()) == 2, len(jobs.jobs()) == 2, len(jobs.orderedJobs()) == 2,
                len(jobs.orderedJobNames()) == 2))

    ordered_jobs = jobs.orderedJobs()
    assert ordered_jobs[0].get('execution', {}).get('exec', None) == '/bin/date', str(ordered_jobs[0])
    assert ordered_jobs[1].get('execution', {}).get('exec', None) == '/bin/echo', str(ordered_jobs[0])
    assert all((ordered_jobs[0].get('name'), ordered_jobs[1].get('name'),
                ordered_jobs[0].get('name') != ordered_jobs[1].get('name')))
    assert all((jobs.clear() == 2, len(jobs.jobs()) == 0))

    jobs.add(name='j1', iterate=10, exec='cat', args=['-v'], stdin='/etc/hostname', stdout='host.out', stderr='host.err',
             numCores=2, numNodes=1, wt='10:00', after='j2')
    assert 'j1' in jobs.jobNames()
    j1_job = jobs.orderedJobs()[0]
    assert all((j1_job.get('name') == 'j1',
                j1_job.get('execution', {}).get('exec') == 'cat',
                j1_job.get('execution', {}).get('args', []) == ['-v'],
                j1_job.get('execution', {}).get('stdin') == '/etc/hostname',
                j1_job.get('execution', {}).get('stdout') == 'host.out',
                j1_job.get('execution', {}).get('stderr') == 'host.err',
                j1_job.get('resources', {}).get('numCores', {}).get('exact') == 2,
                j1_job.get('resources', {}).get('numNodes', {}).get('exact') == 1,
                j1_job.get('resources', {}).get('wt') == '10:00',
                j1_job.get('dependencies', {}).get('after', []) == ['j2'],
                j1_job.get('iterate', {}).get('start', -1) == 0,
                j1_job.get('iterate', {}).get('stop', 0) == 10))
    jobs.clear()

    jobs.add({'exec': 'cat', 'args': ['-v'], 'stdin': '/etc/hostname', 'stdout': 'host.out', 'stderr': 'host.err'},
             name='j1', iterate=10, numCores=2, numNodes=1, wt='10:00', after='j2')
    assert 'j1' in jobs.jobNames()
    j1_job = jobs.orderedJobs()[0]
    assert all((j1_job.get('name') == 'j1',
                j1_job.get('execution', {}).get('exec') == 'cat',
                j1_job.get('execution', {}).get('args', []) == ['-v'],
                j1_job.get('execution', {}).get('stdin') == '/etc/hostname',
                j1_job.get('execution', {}).get('stdout') == 'host.out',
                j1_job.get('execution', {}).get('stderr') == 'host.err',
                j1_job.get('resources', {}).get('numCores', {}).get('exact') == 2,
                j1_job.get('resources', {}).get('numNodes', {}).get('exact') == 1,
                j1_job.get('resources', {}).get('wt') == '10:00',
                j1_job.get('dependencies', {}).get('after', []) == ['j2'],
                j1_job.get('iterate', {}).get('start', -1) == 0,
                j1_job.get('iterate', {}).get('stop', 0) == 10))
    jobs.clear()

    jobs.add({'exec': 'cat', 'args': ['-v'], 'stdin': '/etc/hostname', 'stdout': 'host.out', 'stderr': 'host.err',
             'name': 'j1', 'iterate': 10, 'numCores': 2, 'numNodes': 1, 'wt': '10:00', 'after': 'j2'})
    assert 'j1' in jobs.jobNames()
    j1_job = jobs.orderedJobs()[0]
    assert all((j1_job.get('name') == 'j1',
                j1_job.get('execution', {}).get('exec') == 'cat',
                j1_job.get('execution', {}).get('args', []) == ['-v'],
                j1_job.get('execution', {}).get('stdin') == '/etc/hostname',
                j1_job.get('execution', {}).get('stdout') == 'host.out',
                j1_job.get('execution', {}).get('stderr') == 'host.err',
                j1_job.get('resources', {}).get('numCores', {}).get('exact') == 2,
                j1_job.get('resources', {}).get('numNodes', {}).get('exact') == 1,
                j1_job.get('resources', {}).get('wt') == '10:00',
                j1_job.get('dependencies', {}).get('after', []) == ['j2'],
                j1_job.get('iterate', {}).get('start', -1) == 0,
                j1_job.get('iterate', {}).get('stop', 0) == 10))


def test_api_jobs_std_errors():
    jobs = Jobs()

    with pytest.raises(InvalidJobDescriptionError, match=r".*Missing \"execution/exec\" key.*"):
        jobs.addStd()
    with pytest.raises(InvalidJobDescriptionError, match=r".*Missing \"execution/exec\" key.*"):
        jobs.addStd(name='j1')
    with pytest.raises(InvalidJobDescriptionError, match=r".*Missing \"execution/exec\" key.*"):
        jobs.addStd(name='j1', execution={})
    with pytest.raises(InvalidJobDescriptionError, match=r".*Missing \"execution/exec\" key.*"):
        jobs.addStd(name='j1', execution={'stdout': 'out'})
    with pytest.raises(InvalidJobDescriptionError, match=r".*Missing \"execution/exec\" key.*"):
        jobs.addStd(name='j1', resources={'numCores': { 'exact': 2 }})

    jobs.addStd(name='j1', execution={'exec': '/bin/date'})

    with pytest.raises(InvalidJobDescriptionError, match=r".*Job j1 already in list"):
        jobs.addStd(name='j1', execution={'exec': '/bin/date'})


def test_api_jobs_std():
    jobs = Jobs()

    jobs.addStd(name='j1', iterate={'stop': 10},
                execution={'exec': 'cat', 'args': ['-v'], 'stdin': '/etc/hostname', 'stdout': 'host.out',
                           'stderr': 'host.err'},
                resources={'numCores': {'exact': 2}, 'numNodes': {'exact': 1}, 'wt': '10:00'},
                dependencies={'after': ['j2']})
    assert 'j1' in jobs.jobNames()
    j1_job = jobs.orderedJobs()[0]
    assert all((j1_job.get('name') == 'j1',
                j1_job.get('execution', {}).get('exec') == 'cat',
                j1_job.get('execution', {}).get('args', []) == ['-v'],
                j1_job.get('execution', {}).get('stdin') == '/etc/hostname',
                j1_job.get('execution', {}).get('stdout') == 'host.out',
                j1_job.get('execution', {}).get('stderr') == 'host.err',
                j1_job.get('resources', {}).get('numCores', {}).get('exact') == 2,
                j1_job.get('resources', {}).get('numNodes', {}).get('exact') == 1,
                j1_job.get('resources', {}).get('wt') == '10:00',
                j1_job.get('dependencies', {}).get('after', []) == ['j2'],
                j1_job.get('iterate', {}).get('stop', 0) == 10))
    jobs.clear()


    jobs.addStd({'name': 'j1', 'iterate': {'stop': 10}},
                execution={'exec': 'cat', 'args': ['-v'], 'stdin': '/etc/hostname', 'stdout': 'host.out',
                           'stderr': 'host.err'},
                resources={'numCores': {'exact': 2}, 'numNodes': {'exact': 1}, 'wt': '10:00'},
                dependencies={'after': ['j2']})
    assert 'j1' in jobs.jobNames()
    j1_job = jobs.orderedJobs()[0]
    assert all((j1_job.get('name') == 'j1',
                j1_job.get('execution', {}).get('exec') == 'cat',
                j1_job.get('execution', {}).get('args', []) == ['-v'],
                j1_job.get('execution', {}).get('stdin') == '/etc/hostname',
                j1_job.get('execution', {}).get('stdout') == 'host.out',
                j1_job.get('execution', {}).get('stderr') == 'host.err',
                j1_job.get('resources', {}).get('numCores', {}).get('exact') == 2,
                j1_job.get('resources', {}).get('numNodes', {}).get('exact') == 1,
                j1_job.get('resources', {}).get('wt') == '10:00',
                j1_job.get('dependencies', {}).get('after', []) == ['j2'],
                j1_job.get('iterate', {}).get('stop', 0) == 10))


def test_api_jobs_load_save(tmpdir):
    jobs = Jobs()
    jobs.add(name='j1', iterate=10, exec='cat', args=['-v'], stdin='/etc/hostname', stdout='host.out', stderr='host.err',
             numCores=2, numNodes=1, wt='10:00')
    jobs.add(name='j2', iterate=10, exec='date', args=['-v'], stdout='date.out',
             numCores={'exact': 1}, wt='10:00', after='j1')
    assert 'j1' in jobs.jobNames()
    j1_job = jobs.orderedJobs()[0]
    assert all((j1_job.get('name') == 'j1',
                j1_job.get('execution', {}).get('exec') == 'cat',
                j1_job.get('execution', {}).get('args', []) == ['-v'],
                j1_job.get('execution', {}).get('stdin') == '/etc/hostname',
                j1_job.get('execution', {}).get('stdout') == 'host.out',
                j1_job.get('execution', {}).get('stderr') == 'host.err',
                j1_job.get('resources', {}).get('numCores', {}).get('exact') == 2,
                j1_job.get('resources', {}).get('numNodes', {}).get('exact') == 1,
                j1_job.get('resources', {}).get('wt') == '10:00',
                j1_job.get('iterate', {}).get('stop', 0) == 10))
    assert 'j2' in jobs.jobNames()
    j2_job = jobs.orderedJobs()[1]
    assert all((j2_job.get('name') == 'j2',
                j2_job.get('execution', {}).get('exec') == 'date',
                j2_job.get('execution', {}).get('args', []) == ['-v'],
                j2_job.get('execution', {}).get('stdout') == 'date.out',
                j2_job.get('resources', {}).get('numCores', {}).get('exact') == 1,
                j2_job.get('resources', {}).get('wt') == '10:00',
                j2_job.get('dependencies', {}).get('after', []) == ['j1'],
                j2_job.get('iterate', {}).get('stop', 0) == 10))

    file_path = join(str(tmpdir), 'jobs.json')

    with pytest.raises(FileError):
        jobs.loadFromFile(file_path)
    assert len(jobs.jobs()) == 2

    with pytest.raises(FileError):
        jobs.saveToFile(join(str(tmpdir), 'unexisting_dir', 'jobs.json'))
    assert len(jobs.jobs()) == 2

    jobs.saveToFile(file_path)

    assert all((exists(file_path), stat(file_path).st_size > 0)), file_path

    jobs.clear()
    assert len(jobs.jobs()) == 0

    jobs.loadFromFile(file_path)
    assert len(jobs.jobs()) == 2
    j1_job_clone = jobs.orderedJobs()[0]
    j2_job_clone = jobs.orderedJobs()[1]
    assert all((j1_job_clone == j1_job, j2_job_clone == j2_job))

