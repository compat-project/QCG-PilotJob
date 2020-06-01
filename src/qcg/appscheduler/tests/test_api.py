import pytest

from os import stat
from os.path import join, exists
from shutil import rmtree

import tempfile

from qcg.appscheduler.api.job import Jobs, MAX_ITERATIONS
from qcg.appscheduler.api.errors import InvalidJobDescriptionError, JobNotDefinedError, FileError
from qcg.appscheduler.api.manager import LocalManager
from qcg.appscheduler.tests.utils import find_single_aux_dir
from qcg.appscheduler.slurmres import in_slurm_allocation, get_num_slurm_nodes
from qcg.appscheduler.tests.utils import get_slurm_resources_binded, set_pythonpath_to_qcg_module
from qcg.appscheduler.tests.utils import SHARED_PATH


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

    assert all((len(jobs.job_names()) == 2, len(jobs.jobs()) == 2, len(jobs.ordered_jobs()) == 2,
                len(jobs.ordered_job_names()) == 2))

    ordered_jobs = jobs.ordered_jobs()
    assert ordered_jobs[0].get('execution', {}).get('exec', None) == '/bin/date', str(ordered_jobs[0])
    assert ordered_jobs[1].get('execution', {}).get('exec', None) == '/bin/echo', str(ordered_jobs[0])
    assert all((ordered_jobs[0].get('name'), ordered_jobs[1].get('name'),
                ordered_jobs[0].get('name') != ordered_jobs[1].get('name')))
    assert all((jobs.clear() == 2, len(jobs.jobs()) == 0))

    jobs.add(name='j1', iterate=10, exec='cat', args=['-v'], stdin='/etc/hostname', stdout='host.out', stderr='host.err',
             numCores=2, numNodes=1, wt='10:00', after='j2')
    assert 'j1' in jobs.job_names()
    j1_job = jobs.ordered_jobs()[0]
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
                j1_job.get('iteration', {}).get('start', -1) == 0,
                j1_job.get('iteration', {}).get('stop', 0) == 10))
    jobs.clear()

    jobs.add({'exec': 'cat', 'args': ['-v'], 'stdin': '/etc/hostname', 'stdout': 'host.out', 'stderr': 'host.err'},
             name='j1', iterate=10, numCores=2, numNodes=1, wt='10:00', after='j2')
    assert 'j1' in jobs.job_names()
    j1_job = jobs.ordered_jobs()[0]
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
                j1_job.get('iteration', {}).get('start', -1) == 0,
                j1_job.get('iteration', {}).get('stop', 0) == 10))
    jobs.clear()

    jobs.add({'exec': 'cat', 'args': ['-v'], 'stdin': '/etc/hostname', 'stdout': 'host.out', 'stderr': 'host.err',
             'name': 'j1', 'iterate': 10, 'numCores': 2, 'numNodes': 1, 'wt': '10:00', 'after': 'j2'})
    assert 'j1' in jobs.job_names()
    j1_job = jobs.ordered_jobs()[0]
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
                j1_job.get('iteration', {}).get('start', -1) == 0,
                j1_job.get('iteration', {}).get('stop', 0) == 10))


def test_api_jobs_std_errors():
    jobs = Jobs()

    with pytest.raises(InvalidJobDescriptionError, match=r".*Missing \"execution/exec\" key.*"):
        jobs.add_std()
    with pytest.raises(InvalidJobDescriptionError, match=r".*Missing \"execution/exec\" key.*"):
        jobs.add_std(name='j1')
    with pytest.raises(InvalidJobDescriptionError, match=r".*Missing \"execution/exec\" key.*"):
        jobs.add_std(name='j1', execution={})
    with pytest.raises(InvalidJobDescriptionError, match=r".*Missing \"execution/exec\" key.*"):
        jobs.add_std(name='j1', execution={'stdout': 'out'})
    with pytest.raises(InvalidJobDescriptionError, match=r".*Missing \"execution/exec\" key.*"):
        jobs.add_std(name='j1', resources={'numCores': { 'exact': 2 }})

    jobs.add_std(name='j1', execution={'exec': '/bin/date'})

    with pytest.raises(InvalidJobDescriptionError, match=r".*Job j1 already in list"):
        jobs.add_std(name='j1', execution={'exec': '/bin/date'})


def test_api_jobs_std():
    jobs = Jobs()

    jobs.add_std(name='j1', iterate={'stop': 10},
                execution={'exec': 'cat', 'args': ['-v'], 'stdin': '/etc/hostname', 'stdout': 'host.out',
                           'stderr': 'host.err'},
                resources={'numCores': {'exact': 2}, 'numNodes': {'exact': 1}, 'wt': '10:00'},
                dependencies={'after': ['j2']})
    assert 'j1' in jobs.job_names()
    j1_job = jobs.ordered_jobs()[0]
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


    jobs.add_std({'name': 'j1', 'iterate': {'stop': 10}},
                execution={'exec': 'cat', 'args': ['-v'], 'stdin': '/etc/hostname', 'stdout': 'host.out',
                           'stderr': 'host.err'},
                resources={'numCores': {'exact': 2}, 'numNodes': {'exact': 1}, 'wt': '10:00'},
                dependencies={'after': ['j2']})
    assert 'j1' in jobs.job_names()
    j1_job = jobs.ordered_jobs()[0]
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
    assert 'j1' in jobs.job_names()
    j1_job = jobs.ordered_jobs()[0]
    assert all((j1_job.get('name') == 'j1',
                j1_job.get('execution', {}).get('exec') == 'cat',
                j1_job.get('execution', {}).get('args', []) == ['-v'],
                j1_job.get('execution', {}).get('stdin') == '/etc/hostname',
                j1_job.get('execution', {}).get('stdout') == 'host.out',
                j1_job.get('execution', {}).get('stderr') == 'host.err',
                j1_job.get('resources', {}).get('numCores', {}).get('exact') == 2,
                j1_job.get('resources', {}).get('numNodes', {}).get('exact') == 1,
                j1_job.get('resources', {}).get('wt') == '10:00',
                j1_job.get('iteration', {}).get('stop', 0) == 10))
    assert 'j2' in jobs.job_names()
    j2_job = jobs.ordered_jobs()[1]
    assert all((j2_job.get('name') == 'j2',
                j2_job.get('execution', {}).get('exec') == 'date',
                j2_job.get('execution', {}).get('args', []) == ['-v'],
                j2_job.get('execution', {}).get('stdout') == 'date.out',
                j2_job.get('resources', {}).get('numCores', {}).get('exact') == 1,
                j2_job.get('resources', {}).get('wt') == '10:00',
                j2_job.get('dependencies', {}).get('after', []) == ['j1'],
                j2_job.get('iteration', {}).get('stop', 0) == 10))

    file_path = join(str(tmpdir), 'jobs.json')

    with pytest.raises(FileError):
        jobs.load_from_file(file_path)
    assert len(jobs.jobs()) == 2

    with pytest.raises(FileError):
        jobs.save_to_file(join(str(tmpdir), 'unexisting_dir', 'jobs.json'))
    assert len(jobs.jobs()) == 2

    jobs.save_to_file(file_path)

    assert all((exists(file_path), stat(file_path).st_size > 0)), file_path

    jobs.clear()
    assert len(jobs.jobs()) == 0

    jobs.load_from_file(file_path)
    assert len(jobs.jobs()) == 2
    j1_job_clone = jobs.ordered_jobs()[0]
    j2_job_clone = jobs.ordered_jobs()[1]
    assert all((j1_job_clone == j1_job, j2_job_clone == j2_job))

    rmtree(tmpdir)


def test_api_submit_simple(tmpdir):
    cores = 4

    m = LocalManager(['--wd', str(tmpdir), '--nodes', str(cores), '--log', 'debug'], {'wdir': str(tmpdir)})

    try:
        res = m.resources()
        assert all(('total_nodes' in res, 'total_cores' in res, res['total_nodes'] == 1, res['total_cores'] == cores))

        ids = m.submit(Jobs().
                       add(exec='/bin/date', stdout='date.out')
                       )
        jid = ids[0]
        assert len(m.list()) == 1

        list_jid = list(m.list().keys())[0]
        assert list_jid == jid

        m.wait4(m.list())

        jinfos = m.info_parsed(ids)
        assert all((len(jinfos) == 1, jid in jinfos, jinfos[jid].status  == 'SUCCEED'))

        aux_dir = find_single_aux_dir(str(tmpdir))
        assert all((exists(tmpdir.join('.qcgpjm-client', 'api.log')),
                    exists(join(aux_dir, 'service.log')),
                    exists(tmpdir.join('date.out'))))
        m.remove(jid)

        ids = m.submit(Jobs().
                       add(name='script', script='''
                       hostname --fqdn >> host2.out; date > host.date.out
                       ''', stdout='host.out')
                       )
        jid = ids[0]
        assert len(m.list()) == 1
        m.wait4(m.list())
        jinfos = m.info_parsed(ids)
        assert all((len(jinfos) == 1, jid in jinfos, jinfos[jid].status  == 'SUCCEED'))
        assert all((exists(tmpdir.join('host.out')),
                    exists(tmpdir.join('host2.out')), stat(tmpdir.join('host2.out')).st_size > 0,
                    exists(tmpdir.join('host.date.out')), stat(tmpdir.join('host.date.out')).st_size > 0))

    finally:
        m.finish()
        m.cleanup()

#    rmtree(tmpdir)


def test_api_submit_iterate(tmpdir):
    cores = 4

    m = LocalManager(['--wd', str(tmpdir), '--nodes', str(cores)], {'wdir': str(tmpdir)})

    try:
        res = m.resources()
        assert all(('total_nodes' in res, 'total_cores' in res, res['total_nodes'] == 1, res['total_cores'] == cores))

        iters = 10
        ids = m.submit(Jobs().
                       add(iterate=iters, exec='/bin/date', stdout='date_${it}.out')
                       )
        jid = ids[0]
        assert len(m.list()) == 1

        m.wait4(m.list())

        jinfos = m.info_parsed(ids, withChilds=True)
        assert all((len(jinfos) == 1, jid in jinfos, jinfos[jid].status  == 'SUCCEED'))
        assert all((jinfos[jid].iterations, jinfos[jid].iterations.get('start', -1) == 0,
                    jinfos[jid].iterations.get('stop', 0) == iters, jinfos[jid].iterations.get('total', 0) == iters,
                    jinfos[jid].iterations.get('finished', 0) == iters, jinfos[jid].iterations.get('failed', -1) == 0))
        assert len(jinfos[jid].childs) == iters
        for iteration in range(iters):
            job_it = jinfos[jid].childs[iteration]
            assert all((job_it.iteration == iteration, job_it.name == '{}:{}'.format(jid, iteration),
                        job_it.total_cores == 1, len(job_it.nodes) == 1)), str(job_it)

        assert all(exists(tmpdir.join('date_{}.out'.format(i))) for i in range(iters))
        m.remove(jid)
    finally:
        m.finish()
        m.cleanup()

    rmtree(tmpdir)


def test_api_submit_resources(tmpdir):
    cores = 4

    m = LocalManager(['--wd', str(tmpdir), '--nodes', str(cores)], {'wdir': str(tmpdir)})

    try:
        res = m.resources()
        assert all(('total_nodes' in res, 'total_cores' in res, res['total_nodes'] == 1, res['total_cores'] == cores))

        ids = m.submit(Jobs().
                       add(exec='/bin/date', numCores=2)
                       )
        jid = ids[0]
        assert len(m.list()) == 1
        m.wait4(m.list())

        jinfos = m.info_parsed(ids)
        assert all((len(jinfos) == 1, jid in jinfos, jinfos[ids[0]].status  == 'SUCCEED',
                    jinfos[ids[0]].total_cores == 2))
    finally:
        m.finish()
        m.cleanup()

    rmtree(tmpdir)


def test_api_submit_slurm_resources():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json'], {'wdir': str(tmpdir)})

        cores = 2
        nodes = 2
        ids = m.submit(Jobs().
                       add(exec='/bin/date', stderr='date.stderr', stdout='date.stdout', numCores=cores, numNodes=nodes)
                       )
        jid = ids[0]
        assert len(m.list()) == 1
        m.wait4(m.list())

        jinfos = m.info_parsed(ids)
        assert all((len(jinfos) == 1, jid in jinfos, jinfos[jid].status  == 'SUCCEED',
                    len(jinfos[jid].nodes) == nodes, jinfos[jid].total_cores == cores * nodes))
        assert all(len(node_cores) == cores for node, node_cores in jinfos[jid].nodes.items())

    finally:
        m.finish()
        m.cleanup()

    rmtree(tmpdir)
