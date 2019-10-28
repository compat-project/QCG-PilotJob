from enum import Enum

from qcg.appscheduler.errors import *
from qcg.appscheduler.allocation import CRAllocation, CRBindAllocation, NodeAllocation


class CRType(Enum):
    """
    Consumable resource type
    """
    GPU = 1
    MEM = 2


class CR:
    def __init__(self, crtype, totalCount=0, used=0):
        self.crtype = crtype
        self.totalCount = totalCount
        self.used = used


    @property
    def available(self):
        """
        Number of available resources.

        Returns:
            number of available resources
        """
        return self.totalCount - self.used


    def allocate(self, count):
        """
        Allocate resources.

        Args:
            count (int) - number of resources to allocate

        Return:
            number of allocated resources, or 0 if no resources has been allocated - due to 
            insufficient resources.
        """
        if count <= self.available:
            self.used += count
            return CRAllocation(self.crtype, count)

        return None


    def release(self, cralloc):
        """
        Release allocated consumable resources.

        Args:
            cralloc (CRAllocation) - allocation to release

        Raises:
            InternalError if allocation size is greater than used resources, no resources are released.
        """
        if not isinstance(cralloc, CRAllocation):
            raise InternalError("failed type of CR allocation - {} vs expected CRAllocation".format(type(cralloc).__name__))

        if cralloc.count > self.used:
            raise InternalError("failed to release more resources {} than is allocated {} (CR {})".\
                    format(cralloc.count, self.used, self.crtype.name))
        
        self.used -= cralloc.count


class CRBind:
    def __init__(self, crtype, instances):
        """
        Consumable resource with bindable instances.
        The object tracks allocation of specific instances.

        Args:
            crtype (CRType) - type of CR
            instances (list()) - all available instances
            used (list) - list of used instances
        """

        self.crtype = crtype
        self.totalCount = len(instances)
        self.__free = list(instances)


    @property
    def available(self):
        """
        Number of available resources.

        Returns:
            number of available resources
        """
        return len(self.__free)


    @property
    def used(self):
        """
        Number of used resources.

        Returns:
            number of used resources
        """
        return self.totalCount - self.available


    def allocate(self, count):
        """
        Allocate bindable resources.

        Args:
            count (int) - number of resources to allocate

        Return:
            CRBindAllocation with a list of allocated bindable instances of the resources, or None if no resources
            has been allocated - due to insufficient resources.
        """
        if count <= self.available:
            allocation = CRBindAllocation(self.crtype, self.__free[0:count])
            self.__free = self.__free[count:]
            return allocation

        return None


    def release(self, cralloc):
        """
        Release allocated bindable consumable resources.

        Args:
            cralloc (CRBindAllocation) - allocation to release

        Raises:
            InternalError if 'count' is greater than used resources, no resources are released.
        """
        if not isinstance(cralloc, CRBindAllocation):
            raise InternalError("failed type of CR allocation - {} vs expected CRBindAllocation".format(type(cralloc).__name__))

        if cralloc.count > self.used:
            raise InternalError("failed to release more resources {} than is allocated {} (CR {})".\
                    format(cralloc.count, self.used, self.crtype.name))
        
        self.__free = sorted(self.__free + cralloc.instances)


class Node:

    def __init__(self, name=None, totalCores=0, used=0, coreIds=None, crs=None):
        """
        Node resources.
        This class stores and allocates specific cores. Each core is identified by the number.

        Args:
            name (str) - name of the node
            totalCores (int) - total number of available cores
            used (int) - initial number of used cores
            coreIds (list(int)) - optional core identifiers (the list must have at least 'totalCores' elements)
            crs (map(CRType,CR|CRBind)) - optional consumable resources
        """
        self.__name = name
        self.__totalCores = totalCores

        if coreIds:
            self.__freeCores = list(coreIds[used:totalCores])
        else:
            self.__freeCores = list(range(used, self.__totalCores))

        self.__crs = crs


    def __getName(self):
        return self.__name

    def __getTotalCores(self):
        return self.__totalCores

    def __getUsedCores(self):
        return self.__totalCores - len(self.__freeCores)

    def __getFreeCores(self):
        return len(self.__freeCores)

    def __getCRs(self):
        return self.__crs
    
    def __getStrCRs(self):
        return ', '.join(['{} - {} ({} used))'.format(crtype.name, cr.totalCount, cr.used) for crtype, cr in self.__crs.items()])

    def __str__(self):
        return '{} {} ({} used){}'.format(self.__name, self.__totalCores, self.__getUsedCores(),
                ', CR ({})'.format(self.__getStrCRs()) if self.__crs else '')


    def hasEnoughCrs(self, crs):
        """
        Check if node has enough CR.

        Args:
            crs (dict(CRType,int)) - requested cr's specification

        Returns:
            true - if node contains requested cr's, otherwise false.
        """
        return self.__crs and \
                all([cr in self.__crs for cr in crs]) and \
                 all([v <= self.__crs[cr].available for cr, v in crs.items()])


    def allocateCrs(self, crs):
        """
        Allocate requested crs.

        Args:
            crs (dict(CRType,int)) - requested cr's specification
            
        Returns:
            dict(CRType,CR|CRBind) - with allocated cr's

        Raises:
            NotSufficientResources - if no all resources could be reserved, in that case no resources will be allocated.
        """
        if crs:
            cr_map = dict()

            try:
                for cr, count in crs.items():
                    cr_alloc = self.__crs[cr].allocate(count)
                    if not cr_alloc:
                        self.__crs[cr].release(cr_alloc)
                        raise NotSufficientResources("failed to allocate {} resources @ {} node ({} requested vs {} available)".\
                                format(cr.name, self.__name, count, self.__crs[cr].available))

                    cr_map[cr] = cr_alloc
            except:
                for crtype, cr in cr_map.items():
                    self.__crs[crtype].release(cr)
                raise

            return cr_map

        return None


    def allocateMax(self, maxCores, crs=None):
        """
        Allocate maximum number of cores on a node and specific number of consumable resources.

        Args:
            maxCores (int) - maximum number of cores to allocate
            crs (dict(CRType,int)) - optional specific number of consumable resources

        Returns:
            NodeAllocation with allocated resources, or None if there no any available resources
        """
        if crs:
            # make sure there is enough cr's on the node
            if not self.hasEnoughCrs(crs):
                return None

        nallocated = min(maxCores, self.__getFreeCores())
        if nallocated > 0:
            allocation = self.__freeCores[0:nallocated]
            self.__freeCores = self.__freeCores[nallocated:]

            if self.resources is not None:
                self.resources.nodeCoresAllocated(len(allocation))

            return NodeAllocation(self, allocation, crs=self.allocateCrs(crs))

        return None


    def allocateExact(self, nCores, crs=None):
        """
        Allocate specific number of cores on a node and specific number of consumable resources.

        Args:
            nCores (int) - requested number of cores to allocate
            crs (dict(CRType,int)) - optional specific number of consumable resources

        Returns:
            NodeAllocation with allocated resources, or None if there no any available resources
        """
        if nCores > 0:
            if crs:
                # make sure there is enough cr's on the node
                if not self.hasEnoughCrs(crs):
                    return None

            if nCores <= self.__getFreeCores():
                allocation = self.__freeCores[0:nCores]
                self.__freeCores = self.__freeCores[nCores:]

                if self.resources is not None:
                    self.resources.nodeCoresAllocated(len(allocation))

                return NodeAllocation(self, allocation, crs=self.allocateCrs(crs))

        return None


    def release(self, allocation):
        """
        Release allocation on a node.

        Args:
            allocation (NodeAllocation): allocated resources
        """
        if allocation.ncores > self.__getUsedCores():
            raise InternalError('trying to release more cores than are used on node {}'.format(self.__name))

        self.__freeCores = sorted(self.__freeCores + allocation.cores)

        if allocation.crs:
            if not self.__crs:
                raise InternalError('trying to release crs which are not available on node {}'.format(self.__name))

            for crtype, cr_bind in allocation.crs.items():
                if not crtype in self.__crs:
                    raise InternalError('CR {} not available on a node {}'.format(crtype.name, self.__name))

                self.__crs[crtype].release(cr_bind)


        if self.resources is not None:
            self.resources.nodeCoresReleased(allocation.ncores)


    name = property(__getName, None, None, "name of the node")
    total = property(__getTotalCores, None, None, "total number of cores")
    used = property(__getUsedCores, None, None, "number of used cores")
    free = property(__getFreeCores, None, None, "number of free cores")
    crs = property(__getCRs, None, None, "available CRs")


class ResourcesType(Enum):
    LOCAL = 1
    SLURM = 2


class Resources:

    def __init__(self, rtype, nodes=None, binding=False):
        """
        Available resources set.
        The set stores and tracks nodes with possible different number of available cores.

        :param type: type of resources (ResourcesType)
        :param nodes: list of available nodes
        """
        self.__type = rtype
        self.__binding = binding

        self.__nodes = nodes
        if self.__nodes is None:
            self.__nodes = []

        for node in self.__nodes:
            node.resources = self

        self.__totalCores = 0
        self.__usedCores = 0

        self.__maxCrs = dict()
        self.__totalCrs = dict()

        #		print "initializing %d nodes" % len(nodes)
        self.__computeResourceStatus()

        self.__systemAllocation = None

    def __computeResourceStatus(self):
        total, used = 0, 0
        self.__maxCrs = dict()
        self.__totalCrs = dict()
        for node in self.__nodes:
            total += node.total
            used += node.used

            if node.crs:
                for cr, value in node.crs.items():
                    if cr not in self.__maxCrs or value.available > self.__maxCrs[cr]:
                        self.__maxCrs[cr] = value.available

                    self.__totalCrs[cr] = self.__totalCrs.get(cr, 0) + value.available

        self.__totalCores = total
        self.__usedCores = used

    def __getRType(self):
        return self.__type

    def __getBinding(self):
        return self.__binding

    def __getNodes(self):
        return self.__nodes

    def __getTotalCores(self):
        return self.__totalCores

    def __getUsedCores(self):
        return self.__usedCores

    def __getFreeCores(self):
        return self.__totalCores - self.__usedCores

    def __getMaxCrs(self):
        """
        Return maximum number of CRs on nodes.

        Returns:
            dict(CRType,count) - maximum number of CRs on nodes
        """
        return self.__maxCrs

    def __getTotalCrs(self):
        """
        Return total number of CRs on nodes.

        Returns:
            dict(CRType,count) - total number of CRs on nodes
        """
        return self.__totalCrs

    def allocate4System(self):
        if self.__systemAllocation:
            self.__systemAllocation.release()

            self.__systemAllocation = None

        for node in self.__nodes:
            self.__systemAllocation = node.allocate(1)

            if self.__systemAllocation:
                break

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


    def __str__(self):
        header = '{} ({} used) cores on {} nodes, options ({})\n'.format(
                self.__totalCores, self.__usedCores, len(self.__nodes), 'binding={}'.format(self.__binding))
        return header + '\n'.join([str(node) for node in self.__nodes])

    #		if self.__nodes:
    #			for node in self.__nodes:
    #				result.join("\n%s" % node)
    #		return result

    def nNodes(self):
        return len(self.__nodes)

    rtype = property(__getRType, None, None, "type of resources")
    binding = property(__getBinding, None, None, "cpu binding active")
    nodes = property(__getNodes, None, None, "list of a nodes")
    totalNodes = property(nNodes, None, None, "total number of nodes")
    totalCores = property(__getTotalCores, None, None, "total number of cores")
    usedCores = property(__getUsedCores, None, None, "used number of cores")
    freeCores = property(__getFreeCores, None, None, "free number of cores")
    maxCrs = property(__getMaxCrs, None, None, "maximum number of CRs on nodes")
    totalCrs = property(__getTotalCrs, None, None, "total number of CRs on nodes")
