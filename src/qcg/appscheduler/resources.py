from qcg.appscheduler.errors import *


class Node:
    def __init__(self, name=None, totalCores=0, used=0):
        self.__name = name
        self.__totalCores = totalCores
        self.__usedCores = used
        self.resources = None

    def __getName(self):
        return self.__name

    def __getTotalCores(self):
        return self.__totalCores

    def __setTotalCores(self, total):
        assert total >= 0 and total >= self.__usedCores
        self.__totalCores = total

    def __getUsedCores(self):
        return self.__usedCores

    def __setUsedCores(self, used):
        assert used > 0 and used <= self.__totalCores
        self.__usedCores = used

    def __getFreeCores(self):
        return self.__totalCores - self.__usedCores

    def __str__(self):
        return "%s %d (%d used)" % (self.__name, self.__totalCores, self.__usedCores)


    def allocate(self, cores):
        """
        Allocate maximum number of cores on a node.

        Args:
            cores (int): maximum number of cores to allocate

        Returns:
            int: number of allocated cores
        """
        allocated = min(cores, self.free)
        self.__usedCores += allocated

        if self.resources is not None:
            self.resources.nodeCoresAllocated(allocated)

        return allocated


    def release(self, cores):
        """
        Release specified number of cores on a node.

        Args:
            cores (int): number of cores to release

        Raises:
            InvalidResourceSpec: when number of cores to release exceeds number of of
              used cores.
        """
        if cores > self.__usedCores:
            raise InvalidResourceSpec()

        self.__usedCores -= cores

        if self.resources is not None:
            self.resources.nodeCoresReleased(cores)

    name = property(__getName, None, None, "name of the node")
    total = property(__getTotalCores, __setTotalCores, None, "total number of cores")
    used = property(__getUsedCores, __setUsedCores, None, "number of allocated cores")
    free = property(__getFreeCores, None, None, "number of available cores")


class Resources:

    def __init__(self, nodes=None):
        self.__nodes = nodes
        if self.__nodes is None:
            self.__nodes = []

        for node in self.__nodes:
            node.resources = self

        self.__totalCores = 0
        self.__usedCores = 0

        #		print "initializing %d nodes" % len(nodes)
        self.__computeCores()

    def __computeCores(self):
        total, used = 0, 0
        for node in self.__nodes:
            total += node.total
            used += node.used

        self.__totalCores = total
        self.__usedCores = used

    def __getNodes(self):
        return self.__nodes

    def __getTotalCores(self):
        return self.__totalCores

    def __getUsedCores(self):
        return self.__usedCores

    def __getFreeCores(self):
        return self.__totalCores - self.__usedCores


    def nodeCoresAllocated(self, cores):
        """
        Function called by the node when some cores has been allocated.
        This function should track number of used cores in Resources statistics.

        Args:
            cores (int): number of allocated cores
        """
        self.__usedCores += cores


    def nodeCoresReleased(self, cores):
        """
        Function called by the node when some cores has been released.
        This function should track number of used cores in Resources statistics.

        Args:
            cores (int): number of released cores
        """
        self.__usedCores -= cores


    def releaseAllocation(self, alloc):
        """
        Relase allocated resources.

        Args:
            alloc (Allocation): allocation to release

        Raises:
            InvalidResourceSpec: when number of cores to release on a node is greater
              than number of used cores.
        """
        for node in alloc.nodeAllocations:
            node.node.release(node.cores)

    def __str__(self):
        header = '%d (%d used) cores on %d nodes\n' % (self.__totalCores, self.__usedCores, \
                                                       len(self.__nodes))
        return header + '\n'.join([str(node) for node in self.__nodes])

    #		if self.__nodes:
    #			for node in self.__nodes:
    #				result.join("\n%s" % node)
    #		return result

    def nNodes(self):
        return len(self.__nodes)

    nodes = property(__getNodes, None, None, "list of a nodes")
    totalNodes = property(nNodes, None, None, "total number of nodes")
    totalCores = property(__getTotalCores, None, None, "total number of cores")
    usedCores = property(__getUsedCores, None, None, "used number of cores")
    freeCores = property(__getFreeCores, None, None, "free number of cores")
