from qcg.appscheduler.api.errors import *


class SubmitResult:

    def __init__(self, list):
        """
        Result of job list submission, contains list of job names along with submit result.
        Each element in alist must contain following elements:
            result (str) - 'OK' or Error description

        Attributes:
            list (dict) - list of job names
        """
        if list is None:
            raise InternalError("List of submited jobs is None")

        self.__list = list


    def names(self):
        return self.__list.keys()


    def submitResult(self, job):
        if job not in self.__list:
            raise JobNotDefinedError(job)

        return self.__list[job]['result']


    def allSubmited(self):
        for job in self.__list:
            if not self.__list[job]['result']:
                return False

        return True


class StatusResult:

    def __init__(self, jlist):
        """
        Result of job list status check, contains list of job names along with current status.
        Each element in a list must contain following elements:
            status (str) - current status of the job

        Attributes:
            jlist (dict) - list of job names
        """
        if jlist is None:
            raise InternalError("List of job statuses is None")

        self.__list = jlist


    def names(self):
        return self.__list


    def status(self, job):
        if job not in self.__list:
            raise JobNotDefinedError(job)

        return self.__list[job]['status']

