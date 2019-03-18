from qcg.appscheduler.errors import *
from qcg.appscheduler.scheduleralgo import SchedulerAlgorithm


class Scheduler:
    """
    Resource orchestration.

    Args:
        resources (Resources): available resources

    Attributes:
        __resources (Resources): status of available resources
    """

    def __init__(self, resources):
        assert resources != None

        self.__resources = resources
        self.__schedulerAlg = SchedulerAlgorithm(self.__resources)
        self.__activeAllocations = set()


    def allocateCores(self, min_cores, max_cores=None):
        """
        Create allocation with given number of cores.

        Args:
            min_cores (int): minimum requested number of cores
            max_cores (int): maximum requested number of cores, if None 'min_cores'
                             will mean also 'max_cores'


        Returns:
            Allocation: created allocation
            None: not enough free resources

        Raises:
            NotSufficientResources: when there are not enough resources avaiable
        """
        alloc = self.__schedulerAlg.allocateCores(min_cores, max_cores)

        if alloc is not None:
            self.__activeAllocations.add(alloc)

        return alloc


    def allocateJob(self, resources):
        """
        Create allocation for job with given resources.

        Args:
            resources (JobResources): job's resource requirements

        Returns:
            Allocation: created allocation
            None: not enough free resources

        Raises:
            NotSufficientResources: when there are not enough resources avaiable
            InvalidResourceSpec: when resource requirements are not valid
        """
        alloc = self.__schedulerAlg.allocateJob(resources)

        if alloc is not None:
            self.__activeAllocations.add(alloc)

        return alloc


    def releaseAllocation(self, alloc):
        """
        Release resources assigned for the specificated allocation.

        Args:
            alloc (Allocation): allocation to release

        Raises:
            InvalidAllocation: when the allocation is not registered in the scheduler (it
              might be released earlier)
        """
        if alloc not in self.__activeAllocations:
            raise InvalidAllocation()

        self.__activeAllocations.remove(alloc)
        self.__resources.releaseAllocation(alloc)
