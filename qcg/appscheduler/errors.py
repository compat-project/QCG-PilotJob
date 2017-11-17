
class AppSchedulerError(Exception):
	pass

class SlurmEnvError(AppSchedulerError):
	pass

class InvalidAllocation(AppSchedulerError):
	pass

class InvalidResourceSpec(AppSchedulerError):
	pass

class NotSufficientResources(AppSchedulerError):
	pass

class JobAlreadyExist(AppSchedulerError):
	pass

class IllegalResourceRequirements(AppSchedulerError):
	pass

class IllegalJobDescription(AppSchedulerError):
	pass


