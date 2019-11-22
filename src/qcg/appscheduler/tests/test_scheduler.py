import pytest
#import functools
#import operator

from qcg.appscheduler.resources import CRType, CR, CRBind, Node, ResourcesType, Resources
from qcg.appscheduler.scheduler import Scheduler
from qcg.appscheduler.joblist import JobResources
from qcg.appscheduler.errors import *


def test_scheduler_allocate_cores():
    nCores = [ 8, 8, 8, 8 ]
    r = Resources(ResourcesType.LOCAL, [
            Node("n.{}".format(i), totalCores=nCores[i], used=0) for i in range(len(nCores))
            ])

    s = Scheduler(r)
    assert s

    assert all((len(r.nodes) == len(nCores), [r.nodes[i] == "n.{}".format(i) for i in range(len(nCores))]))
    assert all([r.nodes[i].total == nCores[i] and r.nodes[i].used == 0 and r.nodes[i].free == nCores[i] for i in range(len(nCores))])
    assert all((r.totalCores == sum(nCores), r.freeCores == sum(nCores), r.usedCores == 0))

    c1 = 4
    a1 = s.allocateCores(c1)
    assert a1 and all((a1.cores == c1, len(a1.nodeAllocations) == 1, a1.nodeAllocations[0].cores == list(range(c1)), a1.nodeAllocations[0].crs == None))
    assert all((r.totalCores == sum(nCores), r.freeCores == sum(nCores) - c1, r.usedCores == c1))

    c2_min = 4
    c2_max = 8
    a2 = s.allocateCores(c2_min, c2_max)
    assert a2 and all((a2.cores == c2_max, len(a2.nodeAllocations) == 2,
        a2.nodeAllocations[0].cores == list(range(c1, nCores[0])), a2.nodeAllocations[0].crs == None,
        a2.nodeAllocations[1].cores == list(range(c1 + c2_max - nCores[0])), a2.nodeAllocations[0].crs == None))
    assert all((r.totalCores == sum(nCores), r.freeCores == sum(nCores) - c1 - c2_max, r.usedCores == c1 + c2_max))

    s.releaseAllocation(a1)
    assert all((r.totalCores == sum(nCores), r.freeCores == sum(nCores) - c2_max, r.usedCores == c2_max))

    c3_min = 2
    c3_max = 4
    a3 = s.allocateCores(c3_min, c3_max)
    assert a3 and all((a3.cores == c3_max, len(a3.nodeAllocations) == 1,
        a3.nodeAllocations[0].cores == list(range(c3_max)), a3.nodeAllocations[0].crs == None))
    assert all((r.totalCores == sum(nCores), r.freeCores == sum(nCores) - c2_max - c3_max, r.usedCores == c2_max + c3_max))

    c4 = 20
    a4 = s.allocateCores(c4)
    assert a4 and all((a4.cores == c4, len(a4.nodeAllocations) == 3,
        a4.nodeAllocations[0].cores == list(range(c1 + c2_max - nCores[0], nCores[1])), a4.nodeAllocations[0].crs == None,
        a4.nodeAllocations[1].cores == list(range(nCores[2])), a4.nodeAllocations[1].crs == None,
        a4.nodeAllocations[2].cores == list(range(nCores[3])), a4.nodeAllocations[2].crs == None))
    assert all((r.totalCores == sum(nCores), r.freeCores == sum(nCores) - c2_max - c3_max - c4 == 0, r.usedCores == c2_max + c3_max + c4 == r.totalCores))

    # no more available resources
    c5 = 1
    a5 = s.allocateCores(c5)
    assert a5 == None
    assert all((r.totalCores == sum(nCores), r.freeCores == sum(nCores) - c2_max - c3_max - c4 == 0, r.usedCores == c2_max + c3_max + c4 == r.totalCores))

    c6_min = 1
    c6_max = 2
    a6 = s.allocateCores(c6_min, c6_max)
    assert a6 == None
    assert all((r.totalCores == sum(nCores), r.freeCores == sum(nCores) - c2_max - c3_max - c4 == 0, r.usedCores == c2_max + c3_max + c4 == r.totalCores))

    # release all allocations
    s.releaseAllocation(a2)
    s.releaseAllocation(a3)
    s.releaseAllocation(a4)
    assert all((r.totalCores == sum(nCores), r.freeCores == sum(nCores), r.usedCores == 0))


def test_scheduler_allocate_job_gpus():
    nCores = [ 8, 8, 8, 8 ]
    nGpus = [ 2, 4, 4, 4 ]
    nCrs = [
        { CRType.GPU: CRBind(CRType.GPU, list(range(nGpu))) } for nGpu in nGpus
    ]
    r = Resources(ResourcesType.LOCAL, [
            Node("n.{}".format(i), totalCores=nCores[i], used=0, crs=nCrs[i]) for i in range(len(nCores))
            ])

    s = Scheduler(r)
    assert s

    assert all((len(r.nodes) == len(nCores), [r.nodes[i] == "n.{}".format(i) for i in range(len(nCores))]))
    assert all([r.nodes[i].total == nCores[i] and r.nodes[i].used == 0 and r.nodes[i].free == nCores[i] for i in range(len(nCores))])
    assert all([len(r.nodes[i].crs) == 1 and CRType.GPU in r.nodes[i].crs and r.nodes[i].crs[CRType.GPU].totalCount == nGpus[i] and \
            r.nodes[i].crs[CRType.GPU].used == 0 and r.nodes[i].crs[CRType.GPU].available == nGpus[i] for i in range(len(nCores))])
    assert all((r.totalCores == sum(nCores), r.freeCores == sum(nCores), r.usedCores == 0))

    # allocate half of the first node's cpus and one gpu
    job1_r_c = 4
    job1_r_n = 1
    job1_r_g = 1
    job1_r = JobResources(numCores=job1_r_c, numNodes=job1_r_n, nodeCrs={ 'gpu': job1_r_g })
    assert job1_r

    a1 = s.allocateJob(job1_r)
    assert a1
    assert a1 and all((a1.cores == job1_r_c * job1_r_n, len(a1.nodeAllocations) == job1_r_n,
        a1.nodeAllocations[0].node.name == "n.0", a1.nodeAllocations[0].cores == list(range(job1_r_c))))
    assert a1.nodeAllocations[0].crs and ((CRType.GPU in a1.nodeAllocations[0].crs, a1.nodeAllocations[0].crs[CRType.GPU].count == job1_r_g,
        a1.nodeAllocations[0].crs[CRType.GPU].instances == list(range(job1_r_g))))
    assert all((r.totalCores == sum(nCores), r.freeCores == sum(nCores) - job1_r_c, r.usedCores == job1_r_c))


    # try to allocate half of the node's cpus but with two gpu's - allocation should not fit to the first node
    job2_r_c = 4
    job2_r_n = 1
    job2_r_g = 2
    job2_r = JobResources(numCores=job2_r_c, numNodes=job2_r_n, nodeCrs={ 'gpu': job2_r_g })
    assert job2_r

    a2 = s.allocateJob(job2_r)
    assert a2
    assert a2 and all((a2.cores == job2_r_c * job2_r_n, len(a2.nodeAllocations) == job2_r_n,
        a2.nodeAllocations[0].node.name == "n.1", a2.nodeAllocations[0].cores == list(range(job2_r_c))))
    assert a2.nodeAllocations[0].crs and ((CRType.GPU in a2.nodeAllocations[0].crs, a2.nodeAllocations[0].crs[CRType.GPU].count == job2_r_g,
        a2.nodeAllocations[0].crs[CRType.GPU].instances == list(range(job2_r_g))))
    assert all((r.totalCores == sum(nCores), r.freeCores == sum(nCores) - job1_r_c - job2_r_c, r.usedCores == job1_r_c + job2_r_c))

    # try to allocate node with exceeding gpu amount on node
    job3_r_c = 1
    job3_r_n = 1
    job3_r_g = max(nGpus) + 2
    job3_r = JobResources(numCores=job3_r_c, numNodes=job3_r_n, nodeCrs={ 'gpu': job3_r_g })
    assert job3_r

    with pytest.raises(NotSufficientResources):
        s.allocateJob(job3_r)

    # try to allocate node with exceeding total gpus amount 
    job4_r_c = 1
    job4_r_n = 4
    job4_r_g = max(nGpus)
    job4_r = JobResources(numCores=job4_r_c, numNodes=job4_r_n, nodeCrs={ 'gpu': job4_r_g })
    assert job4_r

    with pytest.raises(NotSufficientResources):
        s.allocateJob(job4_r)
  
    # allocate gpus across many nodes
    job5_r_c = 2
    job5_r_n = 3
    job5_r_g = 2
    job5_r = JobResources(numCores=job5_r_c, numNodes=job5_r_n, nodeCrs={ 'gpu': job5_r_g })
    assert job5_r

    a5 = s.allocateJob(job5_r)
    assert a5 and all((a5.cores == job5_r_c * job5_r_n, len(a5.nodeAllocations) == job5_r_n,
        a5.nodeAllocations[0].node.name == "n.1", a5.nodeAllocations[0].cores == list(range(job2_r_c, job2_r_c + job5_r_c)),
        a5.nodeAllocations[1].node.name == "n.2", a5.nodeAllocations[1].cores == list(range(job5_r_c)),
        a5.nodeAllocations[2].node.name == "n.3", a5.nodeAllocations[2].cores == list(range(job5_r_c)))), str(a5)
    assert a5.nodeAllocations[0].crs and ((CRType.GPU in a5.nodeAllocations[0].crs, a5.nodeAllocations[0].crs[CRType.GPU].count == job5_r_g,
        a5.nodeAllocations[0].crs[CRType.GPU].instances == list(range(job5_r_g))))
    assert a5.nodeAllocations[1].crs and ((CRType.GPU in a5.nodeAllocations[1].crs, a5.nodeAllocations[1].crs[CRType.GPU].count == job5_r_g,
        a5.nodeAllocations[1].crs[CRType.GPU].instances == list(range(job5_r_g))))
    assert a5.nodeAllocations[2].crs and ((CRType.GPU in a5.nodeAllocations[2].crs, a5.nodeAllocations[2].crs[CRType.GPU].count == job5_r_g,
        a5.nodeAllocations[2].crs[CRType.GPU].instances == list(range(job5_r_g))))
    assert all((r.totalCores == sum(nCores), r.freeCores == sum(nCores) - job1_r_c - job2_r_c - job5_r_c * job5_r_n,
        r.usedCores == job1_r_c + job2_r_c + job5_r_c * job5_r_n))

    # allocate cpu's across many nodes
    job6_r_c = 16
    job6_r = JobResources(numCores=job6_r_c)
    assert job6_r

    a6 = s.allocateJob(job6_r)
    assert a6 and all((a6.cores == job6_r_c, len(a6.nodeAllocations) == 4,
        a6.nodeAllocations[0].node.name == "n.0", a6.nodeAllocations[0].cores == list(range(job1_r_c, nCores[0])),
        a6.nodeAllocations[1].node.name == "n.1", a6.nodeAllocations[1].cores == list(range(job2_r_c + job5_r_c, nCores[1])),
        a6.nodeAllocations[2].node.name == "n.2", a6.nodeAllocations[2].cores == list(range(job5_r_c, nCores[2])),
        a6.nodeAllocations[3].node.name == "n.3", a6.nodeAllocations[3].cores == list(range(job5_r_c, job5_r_c + 4)))), str(a6)
    assert ([a6.nodeAllocations[i].crs is None for i in range(4)])
    assert all((r.totalCores == sum(nCores), r.freeCores == sum(nCores) - job1_r_c - job2_r_c - job5_r_c * job5_r_n - job6_r_c,
        r.usedCores == job1_r_c + job2_r_c + job5_r_c * job5_r_n + job6_r_c))

    # release all allocations
    a1.release()
    a2.release()
    a5.release()
    a6.release()

    assert all((len(r.nodes) == len(nCores), [r.nodes[i] == "n.{}".format(i) for i in range(len(nCores))]))
    assert all([r.nodes[i].total == nCores[i] and r.nodes[i].used == 0 and r.nodes[i].free == nCores[i] for i in range(len(nCores))])
    assert all([len(r.nodes[i].crs) == 1 and CRType.GPU in r.nodes[i].crs and r.nodes[i].crs[CRType.GPU].totalCount == nGpus[i] and \
            r.nodes[i].crs[CRType.GPU].used == 0 and r.nodes[i].crs[CRType.GPU].available == nGpus[i] for i in range(len(nCores))])
    assert all((r.totalCores == sum(nCores), r.freeCores == sum(nCores), r.usedCores == 0))

    #print('current resource status: {}'.format(str(r)))

