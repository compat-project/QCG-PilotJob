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
            nodeAlloc = node.allocateMax(max_cores - allocatedCores)

            if nodeAlloc:
                allocation.addNode(nodeAlloc)

                allocatedCores += nodeAlloc.ncores

                if allocatedCores == max_cores:
                    break

        # this should never happen
        if allocatedCores < min_cores or allocatedCores > max_cores:
            allocation.releaseAllocation()
            raise NotSufficientResources()

        return allocation


    def __allocateEntireNodes(self, min_nodes, max_nodes, crs):
        """
        Create allocation with specified number of entire nodes (will all available cores).

        Args:
            min_nodes (int) - minimum number of nodes
            max_nodes (int) - maximum number of nodes
            crs (dict(CRType,int)) - consumable resources requirements per node

        Returns:
            Allocation: created allocation
            None: not enough free resources
        """
        allocation = Allocation()

        for node in self.resources.nodes:
            if node.used == 0:
                #				print("trying to allocate {} cores on on node {}".format(node.total, node.name))
                nAlloc = node.allocateExact(node.total, crs)
                #				print("allocated {} cores".format(ncores))
                if nAlloc.ncores == node.total:
                    allocation.addNode(nAlloc)

                    #					print("already allocated {} nodes from {} maximum".format(len(allocation.nodeAllocations), max_nodes))
                    if len(allocation.nodeAllocations) == max_nodes:
                        break
                else:
                    nAlloc.release()

        if len(allocation.nodeAllocations) >= min_nodes:
            return allocation
        else:
            allocation.releaseAllocation()
            return None


    def __allocateCoresOnNodes(self, min_nodes, max_nodes, min_cores, max_cores, crs):
        """
        Create allocation with specified number of nodes, where on each node should be
        given number of allocated cores.

        Args:
            min_nodes (int): minimum number of nodes
            max_nodes (int): maximum number of nodes
            min_cores (int): minimum number of cores to allocate on each node
            max_cores (int): maximum number of cores to allocate on each node
            crs (dict(CRType, int)) - consumable resource requirements per node

        Returns:
            Allocation: created allocation
            None: not enough free resources
        """
        allocation = Allocation()

#        logging.info("allocating ({},{}) nodes with ({},{}) cores".format(min_nodes, max_nodes, min_cores, max_cores))

        for node in self.resources.nodes:
            if node.free >= min_cores:
                nAlloc = node.allocateMax(max_cores, crs)

#                logging.info("allocated {} cores on a single node".format(nAlloc.ncores if nAlloc else 0))

                if nAlloc:
                    if nAlloc.ncores >= min_cores:
                        allocation.addNode(nAlloc)

                        if len(allocation.nodeAllocations) == max_nodes:
#                            logging.info("already allocated enough {} nodes".format(len(allocation.nodeAllocations)))
                            break
                    else:
                        nAlloc.release()

        if len(allocation.nodeAllocations) >= min_nodes:
            logging.info("allocation contains {} nodes which meets requirements ({} min)".format(
                len(allocation.nodeAllocations), min_nodes))
            return allocation
        else:
            logging.info("allocation contains {} nodes which doesn't meets requirements ({} min)".format(
                len(allocation.nodeAllocations), min_nodes))
            allocation.release()
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
                    "{} exceeds available number of nodes ({})".format(min_nodes, len(self.resources.nodes)))

        min_cores = 0
        total_cores = 0
        if r.hasCores():
            if r.cores.isExact():
                min_cores = r.cores.exact
            else:
                if r.cores.min is None or r.cores.max is None:
                    raise InvalidResourceSpec("Both core's range boundaries (min, max) must be defined")
                min_cores = r.cores.min

            total_cores = min_nodes * min_cores

            if self.resources.totalCores < total_cores:
                raise NotSufficientResources(
                    "{} exceeds available number of cores ({})".format(total_cores, self.resources.totalCores))

            if self.resources.freeCores < total_cores:
                return None

        if min_nodes == 0 and min_cores == 0:
            raise InvalidResourceSpec("Missing node and cores specification")

        if r.hasNodeCrs() and min_nodes == 0:
            raise InvalidResourceSpec("Number of nodes is required when CR are used")

        if r.hasNodeCrs():
            for cr, count in r.crs.items():
                if cr not in self.resources.maxCrs or count > self.resources.maxCrs[cr] or \
                        min_nodes * count > self.resources.totalCrs[cr]:
                    raise NotSufficientResources(
                        "CR {} exceeds maximum available resources on node ({})".format(cr.name, self.resources.maxCrs[cr]))

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
                    "Invalid nodes specification (min: {}, max: {})".format(min_nodes, max_nodes))

            allocation = self.__allocateEntireNodes(min_nodes, max_nodes, r.crs)
        else:
            max_cores = min_cores
            if not r.cores.isExact():
                max_cores = r.cores.max

            if (min_cores == 0 and max_cores == 0) or \
                    max_cores < 1 or \
                    max_cores < min_cores:
                raise InvalidResourceSpec(
                    "Invalid cores specification (min: {}, max: {})".format(min_cores, max_cores))

            if r.hasNodes():
                if r.nodes.isExact():
                    max_nodes = min_nodes
                else:
                    max_nodes = r.nodes.max

            #			logging.info("looking for ({},{}) nodes and ({},{}) cores".format(
            #					min_nodes, max_nodes, min_cores, max_cores))

            if min_nodes == 0:
                # allocate ('min_cores', 'max_cores') on any number of nodes
                allocation = self.allocateCores(min_cores, max_cores)
            else:
                #print("allocateCoresOnNodes - nodes ({} - {}), cores ({} - {}), crs ({})".format(min_nodes, max_nodes, min_cores, max_cores, str(r.crs)))
                # allocate ('min_cores', 'max_cores') on ('min_nodes', 'max_nodes') each
                allocation = self.__allocateCoresOnNodes(min_nodes, max_nodes,
                                                         min_cores, max_cores, r.crs)

        return allocation
