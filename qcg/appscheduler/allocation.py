class NodeAllocation:

    def __init__(self, node, cores):
        """
        Resource allocation on a single node, contains information about a node,
        along with the number of cores allocated.

        Args:
            node (Node): a node definition
            cores (int): number of allocated cores from this node

        Attributes:
            __node: a node definition
            __cores: number of allocated cores
        """
        assert node
        assert cores > 0

        self.__node = node
        self.__cores = cores


    def __getCores(self):
        """
        Return number of allocated cores on a node.

        Returns:
            int: number of cores
        """
        return self.__cores


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
        return "%d @ %s" % (self.__cores, self.__node.name)

    cores = property(__getCores, None, None, "number of cores @ the node")
    node = property(__getNode, None, None, "node")


class Allocation:

    def __init__(self):
        """
        Resource allocation splited (possible) among many nodes.

        Args:

        Attributes:
            __nodes (NodeAllocation[]): list of a single node allocation
            __cores (int): total number of cores on all allocations
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
        self.__cores += nodeAllocation.cores


    def __updateCores(self):
        """
        Compute total number of cores in an allocation.
        """
        cores = 0
        for node in self.__nodes:
            cores += node.cores
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
        return ','.join(["%s:%d" % (node.node.name, node.cores) for node in self.__nodes])

    cores = property(__getCores, None, None, "number of cores")
    nodeAllocations = property(__getNodeAllocations, None, None, "nodes")
