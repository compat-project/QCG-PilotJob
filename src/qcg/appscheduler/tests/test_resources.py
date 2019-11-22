import pytest

from qcg.appscheduler.resources import NodeCores, ResourcesType, Resources


def test_resources_export():
    nodes = [ NodeCores('n1', 10),
              NodeCores('n2', 4, 0, [ 2, 3, 4, 5 ]) ]
    res = Resources(ResourcesType.LOCAL, nodes, False)

    assert all((res.totalNodes == 2, res.totalCores == sum([n.total for n in nodes]), res.usedCores == 0,
                res.freeCores == res.totalCores, res.binding == False, res.rtype == ResourcesType.LOCAL))

    res_copy = Resources.fromDict(res.toDict())
    assert all((res_copy.totalNodes == res.totalNodes, res_copy.totalCores == res.totalCores,
                res_copy.usedCores == res.usedCores, res_copy.freeCores == res.freeCores,
                res_copy.binding == res.binding, res_copy.binding == res.binding))

    for idx, n in enumerate(nodes):
        assert all((n.ids == res_copy.nodes[idx].ids, n.ids == res.nodes[idx].ids,
                    n.freeIds == res_copy.nodes[idx].freeIds, n.freeIds == res.nodes[idx].freeIds))

    assert all((len(res.toJSON()) > 0, res.toJSON() == res_copy.toJSON()))

    nodes = [ NodeCores('n1', 10, 3),
              NodeCores('n2', 4, 2, [ 2, 3, 4, 5 ]) ]
    res = Resources(ResourcesType.SLURM, nodes, True)

    assert all((res.totalNodes == 2,
                res.totalCores == sum([n.total for n in nodes]),
                res.usedCores == sum([n.used for n in nodes]),
                res.freeCores == sum([n.total for n in nodes]) - sum([n.used for n in nodes]),
                nodes[0].freeIds == list(range(3, 10)),
                nodes[1].freeIds == [4, 5],
                res.binding == True, res.rtype == ResourcesType.SLURM)), res.toJSON()

    res_copy = Resources.fromDict(res.toDict())
    assert all((res_copy.totalNodes == res.totalNodes, res_copy.totalCores == res.totalCores,
                res_copy.usedCores == res.usedCores, res_copy.freeCores == res.freeCores,
                res_copy.binding == res.binding, res_copy.binding == res.binding))

    for idx, n in enumerate(nodes):
        assert all((n.ids == res_copy.nodes[idx].ids, n.ids == res.nodes[idx].ids,
                    n.freeIds == res_copy.nodes[idx].freeIds, n.freeIds == res.nodes[idx].freeIds))

    assert all((len(res.toJSON()) > 0, res.toJSON() == res_copy.toJSON()))

    print(res.toJSON())