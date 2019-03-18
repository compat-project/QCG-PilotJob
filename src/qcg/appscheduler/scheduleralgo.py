import logging

from qcg.appscheduler.allocation import Allocation, NodeAllocation
from qcg.appscheduler.errors import *
from qcg.appscheduler.joblist import JobResources


class SchedulerAlgorithm:

    def __init__(self, resources=None):
        self.resources = resources


    def __checkResources(self):
        """
        Validate if the resources has been set.

        Raises:
            InternalError: when resources has not been set
        """
        if self.resources is None:
            raise InternalError("Missing resources in scheduler algorithm")


    def allocateCores(self, min_cores, max_cores=None):
        """
        Create allocation with maximum number of cores from given range.
        The cores will be allocated in a linear method.

        Args:
            min_cores (int): minimum requested number of cores
            max_cores (int): maximum requested number of cores

        Returns:
            Allocation: created allocation
            None: not enough free resources

        Raises:
            NotSufficientResources: when there are not enough resources avaiable
            InvalidResourceSpec: when the min_cores < 0 or min_cores > max_cores
        """
        self.__checkResources()

        if max_cores == None:
            max_cores = min_cores

        if min_cores <= 0 or min_cores > max_cores:
            raise InvalidResourceSpec()

        if self.resources.totalCores < min_cores:
            raise NotSufficientResources()

        if self.resources.freeCores < min_cores:
            return None

        allocation = Allocation()
        allocatedCores = 0
        for node in self.resources.nodes:
            nodeCores = node.allocate(max_cores - allocatedCores)

            if nodeCores > 0:
                allocation.addNode(NodeAllocation(node, nodeCores))

                allocatedCores += nodeCores

                if allocatedCores == max_cores:
                    break

        # this should never happen
        if allocatedCores < min_cores or allocatedCores > max_cores:
            self.resources.releaseAllocation(allocation)
            raise NotSufficientResources()

        return allocation


    def __allocateEntireNodes(self, min_nodes, max_nodes):
        """
        Create allocation with specified number of entire nodes (will all available cores).

        Args:
            min_nodes (int): minimum number of nodes
            max_nodes (int): maximum number of nodes

        Returns:
            Allocation: created allocation
            None: not enough free resources
        """
        allocation = Allocation()

        for node in self.resources.nodes:
            if node.used == 0:
                #				print("trying to allocate %d cores on on node %s" % (node.total, node.name))
                ncores = node.allocate(node.total)
                #				print("allocated %d cores" % (ncores))
                if ncores == node.total:
                    allocation.addNode(NodeAllocation(node, ncores))

                    #					print("already allocated %d nodes from %d maximum" % (len(allocation.nodeAllocations), max_nodes))
                    if len(allocation.nodeAllocations) == max_nodes:
                        break
                else:
                    node.release(ncores)

        if len(allocation.nodeAllocations) >= min_nodes:
            return allocation
        else:
            self.resources.releaseAllocation(allocation)
            return None


    def __allocateCoresOnNodes(self, min_nodes, max_nodes, min_cores, max_cores):
        """
        Create allocation with specified number of nodes, where on each node should be
        given number of allocated cores.

        Args:
            min_nodes (int): minimum number of nodes
            max_nodes (int): maximum number of nodes
            min_cores (int): minimum number of cores to allocate on each node
            max_cores (int): maximum number of cores to allocate on each node

        Returns:
            Allocation: created allocation
            None: not enough free resources
        """
        allocation = Allocation()

        #		logging.info("allocating (%d,%d) nodes with (%d,%d) cores" %
        #				(min_nodes, max_nodes, min_cores, max_cores))

        for node in self.resources.nodes:
            ncores = node.allocate(max_cores)

            #			logging.info("allocated %d cores on a single node" % (ncores))

            if ncores >= min_cores:
                allocation.addNode(NodeAllocation(node, ncores))

                if len(allocation.nodeAllocations) == max_nodes:
                    #					logging.info("already allocated enough %d nodes" % (len(allocation.nodeAllocations)))
                    break
            else:
                node.release(ncores)

        if len(allocation.nodeAllocations) >= min_nodes:
            logging.info("allocation contains %d nodes which meets requirements (%d min)" %
                         (len(allocation.nodeAllocations), min_nodes))
            return allocation
        else:
            logging.info("allocation contains %d nodes which doesn't meets requirements (%d min)" %
                         (len(allocation.nodeAllocations), min_nodes))
            self.resources.releaseAllocation(allocation)
            return None


    def allocateJob(self, r):
        """
        Create allocation for job with given resource requirements.

        Args:
            r (JobResources): job's resource requirements

        Returns:
            Allocation: created allocation
            None: not enough free resources

        Raises:
            NotSufficientResources: when there are not enough resources avaiable
            InvalidResourceSpec: when resource requirements are not valid
        """
        self.__checkResources()

        if r is None or not isinstance(r, JobResources):
            raise InvalidResourceSpec("Missing resource requirements")

        if not r.hasNodes() and not r.hasCores():
            raise InvalidResourceSpec("No resources specified")

        min_nodes = 0
        if r.hasNodes():
            if r.nodes.isExact():
                min_nodes = r.nodes.exact
            else:
                if r.nodes.min is None or r.nodes.max is None:
                    raise InvalidResourceSpec("Both node's range boundaries (min, max) must be defined")
                min_nodes = r.nodes.min

            if min_nodes > len(self.resources.nodes):
                raise NotSufficientResources(
                    "%d exceeds available number of nodes (%d)" % (min_nodes, len(self.resources.nodes)))

        min_cores = 0
        if r.hasCores():
            if r.cores.isExact():
                min_cores = r.cores.exact
            else:
                if r.cores.min is None or r.cores.max is None:
                    raise InvalidResourceSpec("Both core's range boundaries (min, max) must be defined")
                min_cores = r.cores.min

            if min_nodes > 1:
                min_cores = min_nodes * min_cores

            if self.resources.totalCores < min_cores:
                raise NotSufficientResources(
                    "%d exceeds available number of cores (%d)" % (min_cores, self.resources.totalCores))

            if self.resources.freeCores < min_cores:
                return None

        if min_nodes == 0 and min_cores == 0:
            raise InvalidResourceSpec("0 nodes with 0 cores is not valid")

        allocation = Allocation()

        if min_cores == 0:
            # allocate ('min_nodes', 'max_nodes') whole nodes
            max_nodes = min_nodes
            if not r.nodes.isExact():
                max_nodes = r.nodes.max

            if (min_nodes == 0 and max_nodes == 0) or \
                    max_nodes < 1 or \
                    max_nodes < min_nodes:
                raise InvalidResourceSpec(
                    "Invalid nodes specification (min: %d, max: %d)" % (min_nodes, max_nodes))

            allocation = self.__allocateEntireNodes(min_nodes, max_nodes)
        else:
            max_cores = min_cores
            if not r.cores.isExact():
                max_cores = r.cores.max

            if (min_cores == 0 and max_cores == 0) or \
                    max_cores < 1 or \
                    max_cores < min_cores:
                raise InvalidResourceSpec(
                    "Invalid cores specification (min: %d, max: %d)" % (min_cores, max_cores))

            if r.hasNodes():
                if r.nodes.isExact():
                    max_nodes = min_nodes
                else:
                    max_nodes = r.nodes.max

            #			logging.info("looking for (%d,%d) nodes and (%d,%d) cores" %
            #					(min_nodes, max_nodes, min_cores, max_cores))

            if min_nodes == 0:
                # allocate ('min_cores', 'max_cores') on any number of nodes
                allocation = self.allocateCores(min_cores, max_cores)
            else:
                # allocate ('min_cores', 'max_cores') on ('min_nodes', 'max_nodes') each
                allocation = self.__allocateCoresOnNodes(min_nodes, max_nodes,
                                                         min_cores, max_cores)

        return allocation
