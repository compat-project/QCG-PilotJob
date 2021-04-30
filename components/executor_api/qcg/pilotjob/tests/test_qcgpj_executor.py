from qcg.pilotjob.executor_api.qcgpj_executor import QCGPJExecutor


def test_qcgpj_executor_create(tmpdir):
    cores = 2

    with QCGPJExecutor(resources=str(cores)) as e:
        qcgpjm = e.qcgpj_manager
        assert qcgpjm is not None
        res = qcgpjm.resources()
        assert all(('total_nodes' in res, 'total_cores' in res, res['total_nodes'] == 1, res['total_cores'] == cores))
