# Job states

* **QUEUED** when job has been successfully received and parsed by the service
* **SCHEDULED** when resources for the job has been allocated by the scheduler
* **EXECUTING** right before job execution, i.e. right before creation of a new process for the job
* **SUCCEED** right after job's process finished with 0 exit code
* **FAILED** right after job's process finished with other than 0 exit code
* **CANCELED** currently not used
* **OMITTED** when job's will never execute, due to the broken dependencies - the parent job is not registered or finished with different status than expected
