
class CRAllocation:
    def __init__(self, crtype, count):
        """
        Allocation of consumable resources

        Args:
            crtype (CRType) - type of CR
            count (int) - amount of allocated CR
        """
        self.crtype = crtype
        self.count = count


class CRBindAllocation:
    def __init__(self, crtype, instances):
        """
        Allocation of bindable consumable resources

        Args:
            crtype (CRType) - type of CR
            instances (list()) - instances of allocated CR
        """
        self.crtype = crtype
        self.instances = instances

    @property
    def count(self):
        return len(self.instances)


class NodeAllocation:

    def __init__(self, node, cores, crs):
        """
        Resource allocation on a single node, contains information about a node,
        along with the allocated cores and consumable resources.

        Args:
            node (Node): a node definition
            cores ([]int): allocated cores on this node
            crs (list((CRType, count))): list of allocated crs

        Attributes:
            __node (Node): a node definition
            __cores (list(int)): allocated cores
            __crs (dict(CRType,CRAllocation|CRBindAllocation)): allocated crs
        """
        assert node
        assert len(cores) > 0

        self.__node = node
        self.__cores = cores
        self.__crs = crs


    def release(self):
        """
        Release resources allocated in this node allocation.
        """
        if self.__node:
            self.__node.release(self)
            self.__node = None
            self.__cores = None
            self.__crs = None


    def __getCores(self):
        """
        Return allocated cores on a node.

        Returns:
            []int: list of cores
        """
        return self.__cores


    def __getNCores(self):
        """
        Return number of allocated cores on a node.

        Returns:
            int: list of cores
        """
        return len(self.__cores)


    def __getCRs(self):
        """
        Return allocated crs on a node.

        Returns:
            dict((CRType, CRAllocation|CRBindAllocation)) - list of allocated crs
        """
        return self.__crs


    def __getNode(self):
        """
        Return node informations.

        Returns:
            Node: a node information
        """
        return self.__node


    def __str__(self):
        """
        Return a human readable description.

        Returns:
            string: a human readable description
        """
        return "{} @ {}".format(str(self.__cores), self.__node.name)

    cores = property(__getCores, None, None, "cores @ the node")
    ncores = property(__getNCores, None, None, "number of cores @ the node")
    node = property(__getNode, None, None, "node")
    crs = property(__getCRs, None, None, "consumable resources")


class Allocation:

    def __init__(self):
        """
        Resource allocation splited (possible) among many nodes.

        Args:

        Attributes:
            __nodes (NodeAllocation[]): list of a single node allocation
            __cores (int): total number of cores on all nodes
        """
        self.__nodes = []
        self.__cores = 0


    def addNode(self, nodeAllocation):
        """
        Add a node allocation.

        Args:
            nodeAllocation (NodeAllocation): description of an allocation on a single
                node
        """
        assert nodeAllocation

        self.__nodes.append(nodeAllocation)
        self.__cores += nodeAllocation.ncores


    def release(self):
        """
        Release allocated resources.
        Release resources allocated on all nodes in allocation.

        Raises:
            InvalidResourceSpec: when number of cores to release on a node is greater
              than number of used cores.
        """
        for node in self.__nodes:
            node.release()

        self.__nodes = None
        self.__cores = 0


    def __updateCores(self):
        """
        Compute total number of cores in an allocation.
        """
        cores = 0
        for node in self.__nodes:
            cores += node.ncores
        self.__cores = cores


    def __getCores(self):
        """
        Return total number of cores of an allocation

        Returns:
            int: number of cores
        """
        return self.__cores


    def __getNodeAllocations(self):
        """
        Return a list of node allocations

        Returns:
            NodeAllocation[]: list of node allocations
        """
        return self.__nodes


    def __str__(self):
        """
        Return a human readable string

        Returns:
            string: human readable string
        """
        header = "%d cores @ %d nodes\n" % (self.__cores, len(self.__nodes))
        return header + '\n'.join([str(node) for node in self.__nodes])


    def description(self):
        """
        Return a single line description of allocation

        Returns:
            str: a single line description of allocation
        """
        #print('allocation contains {} nodes'.format(len(self.__nodes)))
        #for node in self.__nodes:
        #    print('node: {}, # cores {}, cores type {}, cores {}, list cores {}'.format(node.node.name, len(node.cores), type(node.cores), str(node.cores), str(list(node.cores))))
        return ','.join(["{}[{}]".format(node.node.name, ','.join(str(e) for e in list(node.cores))) for node in self.__nodes])


    cores = property(__getCores, None, None, "number of cores")
    nodeAllocations = property(__getNodeAllocations, None, None, "nodes")
