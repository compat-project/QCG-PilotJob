from os.path import join, abspath, exists
from shutil import rmtree
import logging

from qcg.pilotjob.api.manager import LocalManager
from qcg.pilotjob.api.job import Jobs
from qcg.pilotjob.utils.auxdir import find_single_aux_dir

from qcg.pilotjob.tests.utils import submit_2_manager_and_wait_4_info


def test_resume_tracker_files(tmpdir):
    try:
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json', '--nodes', '4'], {'wdir': str(tmpdir)})

        job_req = {
                   'name': 'host',
                   'execution': {
                       'exec': '/bin/date',
                       'stdout': 'out',
                   },
                   'resources': { 'numCores': { 'exact': 1 } }
                   }
        jobs = Jobs().add_std(job_req)
        submit_2_manager_and_wait_4_info(m, jobs, 'SUCCEED')

        aux_dir = find_single_aux_dir(str(tmpdir))

        assert all(exists(join(aux_dir, fname)) for fname in ['track.reqs', 'track.states']), \
            f"missing tracker files in {aux_dir}"

    finally:
        if m:
            m.finish()
            m.cleanup()

    rmtree(tmpdir)


def test_resume_simple(tmpdir):
    try:
        ncores = 4
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json', '--nodes', str(ncores)],
                         {'wdir': str(tmpdir)})

        its = 10
        job_req = {
            'name': 'sleep',
            'execution': {
                'exec': '/bin/sleep',
                'args': [ '4s' ],
                'stdout': 'out',
            },
            'iteration': { 'stop': its },
            'resources': { 'numCores': { 'exact': 1 } }
        }
        jobs = Jobs().add_std(job_req)
        job_ids = m.submit(jobs)

        # because job iterations executes in order, after finish of 4th iteration, the three previous should also finish
        m.wait4('sleep:3')
        jinfos = m.info_parsed(job_ids, withChilds=True)
        assert jinfos
        jinfo = jinfos['sleep']

        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == ncores, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]

            exp_status = 'SUCCEED'
            if iteration > 3 and iteration < 8:
                exp_status = 'EXECUTING'
            elif iteration > 7:
                exp_status = 'QUEUED'
            assert all((job_it.iteration == iteration,
                        job_it.name == '{}:{}'.format('sleep', iteration),
                        job_it.status == exp_status))

        # kill process
        m.kill_manager_process()
        m.cleanup()

        ncores = 4
        m = LocalManager(['--log', 'debug', '--wd', tmpdir, '--report-format', 'json', '--nodes', str(ncores),
                          '--resume', tmpdir],
                         {'wdir': str(tmpdir)})

        m.wait4all()
        jinfos = m.info_parsed(job_ids, withChilds=True)
        assert jinfos
        jinfo = jinfos['sleep']

        assert all((jinfo.iterations, jinfo.iterations.get('start', -1) == 0,
                    jinfo.iterations.get('stop', 0) == its, jinfo.iterations.get('total', 0) == its,
                    jinfo.iterations.get('finished', 0) == its, jinfo.iterations.get('failed', -1) == 0)), str(jinfo)
        assert len(jinfo.childs) == its
        for iteration in range(its):
            job_it = jinfo.childs[iteration]

            assert all((job_it.iteration == iteration,
                        job_it.name == '{}:{}'.format('sleep', iteration),
                        job_it.status == 'SUCCEED'))
    finally:
        if m:
            m.finish()
            m.cleanup()

    rmtree(tmpdir)