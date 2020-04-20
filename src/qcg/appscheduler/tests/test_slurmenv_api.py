import pytest
import tempfile

from os.path import join, abspath, exists
from shutil import rmtree

from qcg.appscheduler.slurmres import in_slurm_allocation, get_num_slurm_nodes

from qcg.appscheduler.tests.utils import get_slurm_resources_binded, set_pythonpath_to_qcg_module, find_single_aux_dir
from qcg.appscheduler.api.manager import LocalManager
from qcg.appscheduler.api.job import Jobs

from qcg.appscheduler.tests.utils import SHARED_PATH


def test_slurmenv_api_resources():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json'], {'wdir': str(tmpdir)})
        api_res = m.resources()

        assert all(('totalNodes' in api_res, 'totalCores' in api_res))
        assert all((api_res['totalNodes'] == resources.totalNodes, api_res['totalCores'] == resources.totalCores))

        aux_dir = find_single_aux_dir(str(tmpdir))

        assert all((exists(join(tmpdir, '.qcgpjm-client', 'api.log')),
                    exists(join(aux_dir, 'service.log'))))

    finally:
        if m:
            m.finish()
            m.stopManager()
            m.cleanup()
        rmtree(tmpdir)


def test_slurmenv_api_submit_simple():
    if not in_slurm_allocation() or get_num_slurm_nodes() < 2:
        pytest.skip('test not run in slurm allocation or allocation is smaller than 2 nodes')

    resources, allocation = get_slurm_resources_binded()

    set_pythonpath_to_qcg_module()
    tmpdir = str(tempfile.mkdtemp(dir=SHARED_PATH))

    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json'], {'wdir': str(tmpdir)})

        jobs = Jobs().\
            addStd({ 'name': 'host',
                     'execution': {
                         'exec': '/bin/hostname',
                         'args': [ '--fqdn' ],
                         'stdout': 'std.out',
                         'stderr': 'std.err'
                     }})
        ids = m.submit(jobs)
        assert len(ids) == len(jobs.jobNames())

        assert len(m.list()) == len(ids)

        m.wait4all()

        jinfos = m.info(ids)
        assert all(('jobs' in jinfos,
                    len(jinfos['jobs'].keys()) == len(ids)))
        assert all(jid in jinfos['jobs'] and jinfos['jobs'][jid].get('data', {}).get('status', '') == 'SUCCEED' for jid in ids)
    finally:
        if m:
            m.finish()
            m.stopManager()
            m.cleanup()
        rmtree(tmpdir)
