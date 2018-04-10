# The QCG Pilot Manager v 0.2


Author: Piotr Kopta <pkopta@man.poznan.pl>, Tomasz Piontek <piontek@man.poznan.pl>, Bartosz Bosak <bbosak@man.poznan.pl>

Copyright (C) 2017-2018 Poznan Supercomputing and Networking Center


## OVERVIEW
The QCG PilotJob Manager system is designed to schedule and execute many small tasks inside one scheduling system allocation. Direct submission of a large group of jobs to a scheduling system can result in long aggregated time to finish as each single job is scheduled independently and waits in a queue. On the other hand the submission of a group of tasks can be restricted or even forbidden by administrative policies defined on clusters.
One can argue that there are available job array mechanisms in many systems, however the traditional job array mechanism allows to run only bunch of jobs having the same resource requirements while tasks being parts of a multiscale simulation by nature vary in requirements and therefore need more flexible solutions.

From the scheduling system perspective, QCG PilotJob Manager is seen as a single job inside a single user allocation. In the other words, the manager controls an execution of a complex experiment consisting of many tasks on resources reserved for the single job allocation. The manager listens to user's requests and executes commands like submit job, cancel job and report resources usage. In order to manage the resources and jobs the system takes into account both resources availability and mutual dependencies between tasks . Two interfaces are defined to communicate with the system: file-based and network-based. The former one is dedicated and more convenient for a static pilot job when a number of jobs is known in advance to the QCG PilotJob Manager start. The network interface is more general and flexible as it allows to dynamically send new requests and track execution of previously submitted jobs during the run-time. 

## MODULES
QCG Pilot Job Manager consists of the following internal functional modules:
 - **Queue** - the queue containing jobs waiting for resources,
 - **Scheduler** algorithm - the algorithm selecting jobs and assigning resources to them. 
 - **Registry** - the permanent registry containing information about all (current and historical) jobs in the system,
 - **Executor** - a module responsible for execution of jobs for which resources were assigned.

## QUEUE & SCHEDULER
All the jobs submitted to the QCG PilotJob Manger system are placed in the queue in the order of they arrival. The scheduling algorithm of QCG PilotJob Manager works on that queue. The goal of the Scheduler is to determine the order of execution and amount of resources assigned to individual jobs to maximise the throughput of the system. The algorithm is based on the following set of rules:
 - Jobs being in the queue are processed in the FIFO manner,
 - For every feasible (ready for execution) job the maximum (possible) amount of requested resources is determined. If the amount of allocated resources is greater than the minimal requirements requested by the user, the resources are exclusively assigned to the job and the job is removed from the queue to be executed. 
 - If the minimal resource requirements are greater than total available resources the job is removed from the queue with the `FAILED` status.
 - If the amount of resources doesn't allow to start the job, it stays in the queue with the `QUEUED` status to be taken into consideration again in the next scheduling iteration,
 - Jobs waiting for successful finish of any other job, are not taken into consideration and stay in the queue with the `QUEUED` state,
 - Jobs for which dependency constraints can not be met, due to failure or cancellation of at least one job which they depend on,  are marked as `OMITTED` and removed from the queue, 
 - If the algorithm finishes processing the given job and some resources still remain unassigned the whole procedure is repeated for the next job.


## EXECUTOR
The QCG PilotJob Manager module called Executor is responsible for execution and control of jobs by interacting with the cluster resource management system. The current implementation is integrated with the SLURM system, but the modular approach allows for relatively easy integration also with other queuing systems. The PilotJob Manager and all the jobs controlled by it are executed in a single allocation. To hide this fact from the individual job and to give it an impression that it is executed directly by the queuing system a set of environment variables, typically set by the queuing system, is overwritten and passed to the job. These variables give the application all typical information about a job it can be interested in, e.g. the amount of assigned resources. In case of parallel application an appropriate machine file is created with a list of resources for each job.

### SLURM EXECUTION ENVIRONMENT
For the SLURM scheduling system, an execution environment for a single job contains the following set of variables:
- `SLURM_NNODES` - a number of nodes
- `SLURM_NODELIST` - a list of nodes separated by the comma
- `SLURM_NPROCS` - a number of cores
- `SLURM_NTASKS` - see `SLURM_NPROCS`
- `SLURM_JOB_NODELIST` - see `SLURM_NODELIST`
- `SLURM_JOB_NUM_NODES` - see `SLURM_NNODES`
- `SLURM_STEP_NODELIST` - see `SLURM_NODELIST`
- `SLURM_STEP_NUM_NODES` - see `SLURM_NNODES`
- `SLURM_STEP_NUM_TASKS` - see `SLURM_NPROCS`
- `SLURM_NTASKS_PER_NODE` - a number of cores on every node listed in `SLURM_NODELIST` separated by the comma,
- `SLURM_STEP_TASKS_PER_NODE` - see `SLURM_NTASKS_PER_NODE`
- `SLURM_TASKS_PER_NODE` - see `SLURM_NTASKS_PER_NODE`

### QCG EXECUTION ENVIRONMENT
To unify the execution environment regardless of the queuing system a set of variables independent from a queuing system is passed to the individual job:
- `QCG_PM_NNODES` - a number of nodes
- `QCG_PM_NODELIST`- a list of nodes separated by the comma
- `QCG_PM_NPROCS` - a number of cores
- `QCG_PM_NTASKS` - see `QCG_PM_NPROCS`
- `QCG_PM_STEP_ID` - a unique identifier of a job (generated by QCG PilotJob Manager)
- `QCG_PM_TASKS_PER_NODE` - a number of cores on every node listed in `QCG_PM_NODELIST` separated by the comma
- `QCG_PM_ZMQ_ADDRESS` - an address of the network interface of QCG PilotJob Manager (if enabled)

## FILE BASED INTERFACE
The "File" interface allows a static sequence of commands (called requests) to be read from a file and performed by the system.

### REQUEST FILE
The request file is a JSON format file containing a sequence of commands (requests). The file must be staged into the working directory of the QCG PilotJob Manager job and passed as an argument of this job invocation. The requests are read in an order they are placed in the file. In the file mode, QCG PilotJob Manager outputs all responses to the log file.

### REQUESTS (Commands)
The request is a JSON dictionary with the `request` key containing a request command. The additional data format depends on a specific request command. The following commands are currently supported:

#### submit Command
Submit a list of jobs to be processed by the system. The `jobs` key must contain a list of formalised descriptions of jobs.


##### Job description format
The Job description is a dictionary with the following keys:
- `name` (required) `String` - job name, must be unique
- `iterate` (optional) `Array of Number` - defines a loop for iterative jobs, the two element array with the start and stop index; the total number of iterations will be *stop_index - start_index* (the last index of the sub-job will be *stop_index - 1*)
- `execution` (required) `Dict` - execution description with the following keys:
  - `exec` (required) `String` - executable name (if available in *$PATH*) or absolute path to the executable,
  - `args` (optional) `Array of String` - list of arguments that will be passed to
    the executable,
  - `env` (optional) `Dict (String: String)` - environment variables that will
    be appended to the execution environment,
  - `wd` (optional) `String` - a working directory, if not defined the
    working directory (current directory) of QCG PilotJob Manager will be used. If
    the path is not absolute it is relative to the QCG PilotJob Manager
    working directory. If the directory pointed by the path does not exist, it is created before 
    the job starts.
  - `stdin`, `stdout`, `stderr` (optional) `String` - path to the
    standard input , standard output and standard error files respectively. 
- `resources` (optional) `Dict` - resource requirements, a dictionary with the following keys: 
  - `numCores` (optional) `Dict` - number of cores,
  - `numNodes` (optional) `Dict`- number of nodes,
    The specification of `numCores`/`numNodes` elements may contain the following keys:
    - `exact` (optional) `Number` - the exact number of cores,
    - `min` (optional) `Number` - minimal number of cores,
    - `max` (optional) `Number` - maximal number of cores,
    (the `exact` and `min` /  `max` are mutually exclusive)

	If `resources` is not defined, the `numCores` with `exact` set to 1 is taken as the default value.

    The `numCores` element without `numNodes` specifies requested number of cores on any number of nodes. The same element used along with the `numNodes` determines the number of cores on each requested node.

    Both elements, `numCores` and `numNodes`, may contain special, optional key `split-into` `Number` along with `min` key. The purpose of this element is to split total number of nodes/cores into smaller chunks for a single job. The quotient of a total number of available cores/nodes by the `split-into` value means number of requested cores/nodes per single chunk.  The `min` keyword restricts number of requested resources to the range:
    > < min, Total_Num / split-into \>

- `dependencies` (optional) `Dict` - a dictionary with the following items:
  - `after` (required) `Array of String` - list of names of jobs that must finish
    before the job can be executed. Only when all listed jobs finish (with SUCCESS)
    the current job is taken into consideration by the scheduler and can be executed.

##### Job description variables
The job description may contain variables in the format:

```${ variable-name }```

which are replaced with appropriate values by QCG PilotJob Manager.

The following set of variables is supported during a request validation:
- `rcnt` - a request counter that is incremented with every request (for iterative sub-jobs the value of this variable is the same)
- `uniq` - a unique identifier of each request (each iterative sub-job has its own unique identifier)
- `sname` - a local cluster name
- `date` - a date when the request was received
- `time` - a time when the request was received
- `dateTime` - date and time when the request was received
- `it` - an index of a current sub-job (only for iterative jobs) 
- `its` - a number of iterations (only for iterative jobs)
- `it_start` - a start index of iterations (only for iterative jobs)
- `it_stop` - a stop index of iterations (only for iterative jobs)
- `jname` - a final job name after substitution of all other used variables to their values

The following variables are handled when resources has been already allocated and before the start of  job execution:
- `root_wd` - a working directory of QCG PilotJob Manager, the parent directory for all relative job's working directories
- `ncores` - a number of allocated cores for the job
- `nnodes` - a number of allocated nodes for the job
- `nlist` - a list of nodes allocated for the job separated by the comma

The sample submit job request is presented below:
```json
{
    "request": "submit",
    "jobs": [
    {
        "name": "msleep2",
        "execution": {
          "exec": "/bin/sleep",
          "args": [
            "5s"
          ],
          "env": {},
          "wd": "sleep.sandbox",
          "stdout": "sleep2.${ncores}.${nnodes}.stdout",
          "stderr": "sleep2.${ncores}.${nnodes}.stderr"
        },
        "resources": {
          "numCores": {
            "exact": 2
          }
        }
      }
   }
   ]
}  
```

The example response is presented below:
```json
  "code": 0,
  "message": "2 jobs submitted",
  "data": {
    "jobs": [
      "echo",
      "msleep2"
    ],
    "submitted": 2
  }
}
```

#### listJobs Command
Return a list of registered jobs. No additional arguments are needed.
The example list jobs request is presented below:
```json
{
	"request": "listJobs"
}
```


The example response is presented below:
```json
{
  "data": {
    "jobs": {
      "msleep2": {
        "status": "EXECUTING"
      },
      "echo": {
        "status": "EXECUTING"
      },
      "env_1": {
        "status": "EXECUTING"
      }
    },
    "length": 3
  },
  "code": 0
}
```

#### jobStatus Command
Report current status of a given jobs. The `jobNames` key must contain a list of job names for which status should be reported. A single job may be in one of the following states:
- `QUEUED` - a job was submitted but there are no enough available resources
- `EXECUTING` - a job is currently executed
- `SUCCEED` - a finished with 0 exit code
- `FAILED` - a job could not be started (for example there is no executable) or a job finished with non-zero exit code or a requested amount of resources exceeds a total amount of resources,
- `CANCELED` - a job has been cancelled either by a user or by a system
- `OMITTED` - a job will never be executed due to the dependencies (a job which this job depends on failed or was cancelled).

The example job status request is presented below:
```json
  {
    "request": "jobStatus",
    "jobNames": [ "msleep2", "echo" ]
  }
```

The example response is presented below:
```json
{
  "data": {
    "jobs": {
      "msleep2": {
        "status": 0,
        "data": {
          "jobName": "msleep2",
          "status": "EXECUTING"
        }
      },
      "echo": {
        "status": 0,
        "data": {
          "jobName": "echo",
          "status": "EXECUTING"
        }
      }
    }
  },
  "code": 0
}
```
The `status` key at the top, job's level contains numeric code that represents
the operation return code - 0 means success, where other values means problem
with obtaining job's status (e.g. due to the missing job name).


#### jobInfo Command
Report detailed information about jobs. The `jobNames` key must contain a list of job names for which information should be reported.
The example job status request is presented below:
```json
  {
    "request": "jobInfo",
    "jobNames": [ "msleep2", "echo" ]
  }
```

The example response is presented below:
```json
{
  "data": {
    "jobs": {
      "msleep2": {
        "status": 0,
        "data": {
          "history": "\n2018-03-14 14:51:05.115960: QUEUED\n2018-03-14 14:51:05.116719: EXECUTING",
          "jobName": "msleep2",
          "runtime": {
            "allocation": "e0128:1",
            "wd": "/home/plgrid/plgkopta/qcg-pilotmanager/qcg-pilotmanager"
          },
          "status": "EXECUTING"
        }
      },
      "echo": {
        "status": 0,
        "data": {
          "history": "\n2018-03-14 14:51:05.116166: QUEUED\n2018-03-14 14:51:05.117643: EXECUTING",
          "jobName": "echo",
          "runtime": {
            "allocation": "e0128:1",
            "wd": "/home/plgrid/plgkopta/qcg-pilotmanager/qcg-pilotmanager"
          },
          "status": "EXECUTING"
        }
      }
    }
  },
  "code": 0
}
```

#### control Command
Controls behaviour of QCG PilotJob Manager. The specific command must be placed in the`command` key. Currently the following commands are supported:
- `finishAfterAllTasksDone`  This command tells QCG PilotJob Manager to wait until all submitted jobs finish. By default, in the file mode, the QCG PilotJob Manager application  finishes as soon as all requests are read from the request file.

The sample control command request is presented below:
```json
  {
    "request": "control",
    "command": "finishAfterAllTasksDone"
  }
```

#### cancelJob Command
Cancel a job with a name specified in the `jobName` key. Currently not supported.
The example cancel job request is presented below:
```json
{
    "request": "cancelJob",
    "jobName": "msleep2"
}
```

#### removeJob Command
Remove a job from the registry. The name of a job to be removed must be placed in the `jobName` key. This request can be used in case when there is a need to submit another job with the same name - because all the job names must be unique a new job cannot be submitted with the same name unless the previous one is removed from the registry.
The example remove job request is presented below:
```json
{
    "request": "removeJob",
    "jobNames": [ "msleep2", "echo" ]
}
```

The example response is presented below:
```json
{
  "data": {
    "removed": 2
  },
  "code": 0
}
```

#### resourcesInfo Command
Return current usage of resources. The information about a number of
available and used nodes/cores is reported. No additional arguments are needed.
The example resources info request is presented below:
```json
{
        "request": "resourcesInfo"
}
```


The example response is presented below:
```json
{
  "data": {
    "totalCores": 8,
    "totalNodes": 1,
    "usedCores": 2,
    "freeCores": 6
  },
  "code": 0
}
```

#### finish Command
Finish the QCG PilotJob Manager application immediately. The jobs being currently executed are killed. No additional arguments are needed. 
The example finish command request is presented below:
```json
{
        "request": "finish"
}
```



## ADDITIONAL OUTPUT FILES
QCG PilotJob Manager creates the following files in a working directory:
- `service.log` - containing service logs, very useful to debug service and jobs
 execution
- `jobs.report` - holding information about all finished (with success or
 failure) jobs along with the details on status, scheduled nodes/cores,
 working directory and wall time; the sample content is presented below:
```
 msleep2 (SUCCEED)
    2018-01-11 16:02:27.740711: QUEUED
    2018-01-11 16:02:27.741276: EXECUTING
    2018-01-11 16:02:33.832568: SUCCEED
    allocation: e0003:2
    wd: /home/plgrid/plgkopta/qcg-pilotmanager-tests/sandbox/sleep2.sandbox
    rtime: 0:00:06.082136
```

## PYTHON VERSION ISSUES
The current implementation of the QCG PilotJob Manager service is based on Python 3. Due to the fact that all jobs controlled by the Manger inherit an environment, the default version of Python for all jobs is also v3. In situation where different version of Python is required by a job, the Python module should be changed in a job's script.

## USAGE
The QCG PilotJob Manager service is executed as a regular job in a queuing system. It exploits a single system allocation for itself and all jobs it controls. For the user convenience, the service has been registered as QCG application (currently only on cluster Eagle @ PSNC) under the name `qcg-pm`. The only and required argument for this application is the name (path) of the requests file. The example script (in QCG Simple description format) is presented below:
```bash
    #QCG host=eagle
    #QCG note=QCG PM bac16
    
    #QCG output=${JOB_ID}.output
    #QCG error=${JOB_ID}.error
    
    #QCG stage-in-file=bac16.json
    #QCG stage-in-file=bac-namd.sh
    #QCG stage-in-file=bac-amber.sh
    #QCG stage-in-file=bac-16-input.tgz
    
    #QCG stage-out-dir=.->out/eagle-pm-bac16-${JOB_ID}
    
    #QCG preprocess=tar --strip-components=1 -x -z -f bac-16-input.tgz
    
    #QCG nodes=4:28
    #QCG queue=fast
    #QCG walltime=PT60M
    #QCG grant=compatpsnc2
    
    #QCG application=qcg-pm
    #QCG argument=bac16.json
```
In the presented example the job requires 4 nodes (28 cores each), a set of input files is staged into the working directory (the `bac16.json` file is the request file containing jobs descriptions). The name of this file is the only argument for the QCG PilotJob Manager application.


In the `bac16.json` we define the following requests (commands):
```json
[
{
    "request": "submit",
    "jobs": [
    {
        "name": "namd_bac16_${it}",
        "iterate": [ 1, 17 ],
        "execution": {
          "exec": "bash",
          "args": [ "${root_wd}/bac-namd.sh", "${it}" ],
          "stdout": "logs/${jname}.stdout",
          "stderr": "logs/${jname}.stderr"
        },
        "resources": {
          "numNodes": {
                "min": 1,
                "max": 2
          }
        }
    },
    {
        "name": "amber_bac16_${it}",
        "iterate": [ 1, 17 ],
        "execution": {
          "exec": "bash",
          "args": [ "${root_wd}/bac-amber.sh", "${it}" ],
          "stdout": "logs/${jname}.stdout",
          "stderr": "logs/${jname}.stderr"
        },
        "resources": {
          "numCores": {
                "exact": 4
          }
        },
        "dependencies": {
          "after": [ "namd_bac16_${it}" ]
        }
    }
    ]
},
{
    "request": "control",
    "command": "finishAfterAllTasksDone"
}
]
```
The file contains two request: submit jobs and control ones. The submit request
defines two iterative jobs. Each iteration of the second job (amber) depends on
the corresponding iteration of the first sub-job (namd).  The control request
is used to tell the Manager to finish after all jobs will be handled and
finished (with any result).


## API
The QCG PilotJob Manager provides a Python API which allows users to dynamically control execution of Pilot Jobs. The user program is run inside an allocation as one of the Pilot job. The communication is done through the network interface provided by the QCG PilotJob Manager. The Python API is provided in the `qcg.appscheduler.api` package.

### Manager
The `qcg.appscheduler.api.Manager` class is a main actor of the API. This class handles an initialization and communication with the QCG PilotJob Manager single instance. It provides methods for:
- obtaining resources status (`resources` method)
- job submission (`submit` method),
- listening all jobs (`list` method),
- obtaining current status of jobs (`status` method),
- obtaining detailed information about jobs (`info` method),
- removing jobs (`remove` method),
- synchronization of job execution (`wait4` method).


#### Initialization
```python
def __init__(address = None, cfg = { })
```

In order to initialize the `Manager` class, the address of the QCG PJM instance must be available. In case where the client application is run as a one of job in QCG PJM, this address is exported in `QCG_PM_ZMQ_ADDRESS`, and is automatically used by the API when user has not provided the address explicitely.

During the initialization of the `Manager` class the user can specify some configuration variables that will influence a behavior of the class. Currently the following configuration keys are supported:
* `poll_delay` - the delay between following status polls in wait methods,
* `log_file` - the location of the log file
* `log_level` - the log level (e.g. `DEBUG`); by default the log level is set to INFO


#### `resources`
```python
def resources()
```

This method returns information about current status of resources available in the QCG PJM. The output format is described in [this section](#resourcesInfo-command)


#### `submit`
```python
    """
    Submit a jobs.

    Args:
        jobs (qcg.appscheduler.api.Jobs) - a job description list

    Returns:
        list - a list of submitted job names

    Raises:
        InternalError - in case of unexpected result format
        ConnectionError - if connection has not been established yet or non zero exit code
    """
	def submit(jobs)
```
Submit a list of jobs. The jobs to submit are stored in a `Jobs` object. In case of success the list of submitted job names is returned. The resulted list can be used in other methods to get actual status of jobs or synchronize their execution.


##### `qcg.appscheduler.api.Jobs`
```python
    """
    Add a new, simple job description to the group.
    If both arguments are present, they are merged and processed as a single dictionary.

    Args:
        dAttrs (dict) - attributes as a dictionary in a simple format
        stdAttrs (dict) - attributes as a named arguments in a simple format

    Raises:
        InvalidJobDescription - in case of non-unique job name or invalid job description
    """
    def add(self, dAttrs = None, **attrs)
	

    """
    Add a new, standard job description (acceptable by the QCG PJM) to the group.
    If both arguments are present, they are merged and processed as a single dictionary.

    Args:
        dAttrs (dict) - attributes as a dictionary in a standard format
        stdAttrs (dict) - attributes as a named arguments in a standard format

    Raises:
        InvalidJobDescription - in case of non-unique job name or invalid job description

    """
    def addStd(self, dAttrs = None, **stdAttrs)

	
    """
    Remote a job from the group.

    Args:
        name (str) - name of the job to remove

    Raises:
        JobNotDefined - in case of missing job in a group with given name
    """
    def remove(self, name)


    """
    Read job's descriptions in format acceptable (StdJob) by the QCG-PJM

    Args:
        formattedDoc (list) - data read from the JSON document, it should be a list with the
          job descriptions - this is the value of 'jobs' key in 'submit' request

    Raises:
        InvalidJobDescription - in case of invalid job description
    """
    def loadFromFile(self, filePath)


    """
    Save job list to file in a JSON format.

    Args:
        fileName (str) - path to the destination file

    Raises:
        FileError - in case of problems with opening / writing output file.
    """
    def saveToFile(self, fileName)
```
The `Jobs` object holds a list of jobs description. The names of jobs must be uniqe.  The jobs can be described in a format presented in [this section](#submit-command), as well as a simplified format. The main change is a more flat out structure of job description data, where elements of structures `execution` and `resources` has been moved to the top level. The other change is also simplified `dependencies` section, which has been replaced by the `after` element, that contains a list of ancestor jobs.

The simplified job description format contains following keys:
* `name` (required - `True`, allowed types - `str`) - the job name,
* `exec` (required - `True`, allowed types - `str`) - path to the exectuable,
* `args` (required - `False`,allowed types - `list`, `str`) - a list of (or single entry) of arguments,
* `stdin` (required - `False`,allowed types - `str) - a path to the standard input file,
* `stdout` (required - `False`,allowed types - `str`) - a path to the standard output file,
* `stderr` (required - `False`,allowed types - `str`) - a path to the standard error file,
* `wd` (required - `False`,allowed types - `str`) - a path to the working directory,
* `numNodes` (required - `False`,allowed types - `dict`) - numer of nodes requirements (described in [section](#job-description-format)),
* `numCores` (required - `False`,allowed types - `dict`) - number of cores requirements (described in [section](#job-description-format)),
* `wt` (required - `False`,allowed types - `str`) - a wall-time specification,
* `iterate` (required - `False`,allowed types - `list`) - iterations description, a list must contain a two, or three following elements: `start iteration`, `end iteration` and optionally, `the step iteration`,
* `after` (required - `False`,allowed types - `list`, `str`) - a list of (or single entry) of tasks that should finish before current one starts,


#### `list`
```python
    """
    List all the jobs.
    Return a list of all job names along with their status.

    Returns:
        list - list of jobs with additional data in format described in 'listJobs' method in QCG PJM.

    Raises:
        InternalError - in case of unexpected result format
        ConnectionError - if connection has not been established yet or non zero exit code
    """
    def list(self)
```
This method returns a list of all submitted and registered (not yet removed) jobs from the QCG PilotJob Manager instance along with their status. The output format is described [in](#listJobs-command).


#### `status`
```python
    """
    Return current status of jobs.

    Args:
        names (list, str) - list of job names

    Returns:
        list - a list of job's status in the format described in 'jobStatus' method of QCG PJM.

    Raises:
        InternalError - in case of unexpected result format
        ConnectionError - if connection has not been established yet or non zero exit code
    """
    def status(self, names)
```
This method returns a status of specified jobs. The output format is described [in](#jobstatus-command).


#### `info`
```python
    """
    Return detailed information about jobs.

    Args:
        names (list, str) - a list of job names

    Returns:
        list - a list of job's detailed information in the format described in 'jobStatus' method of
          QCG PJM.

    Raises:
        InternalError - in case of unexpected result format
        ConnectionError - if connection has not been established yet or non zero exit code
    """
    def info(self, names)
```
The `info` method returns detailed information about specified jobs. The output format is described [in](#jobinfo-command).


#### `remove`
```python
    """
    Remove jobs from registry.

    Args:
        names (list, str) - a list of job names

    Raises:
        InternalError - in case of unexpected result format
        ConnectionError - if connection has not been established yet or non zero exit code
    """
    def remove(self, names)
```
The `remove` method removes a specified jobs from the registry of QCG PilotJob Manager. The output format is described [in][#removejob-command).


#### `wait4`
```python
    """
    Wait for finish of specific jobs.
    This method waits until all specified jobs finish its execution (successfully or not).
    The QCG PJM is periodically polled about status of not finished jobs. The poll interval (2 sec by
    default) can be changed by defining a 'poll_delay' key with appropriate value (in seconds) in
    configuration of constructor.

    Args:
        names (list, str) - a list of job names

    Returns:
        dict - a map with job names and their terminal status

    Raises:
        InternalError - in case of unexpected response
        ConnectionError - in case of connection problems
        see status
    """
    def wait4(self, names)
```
The `wait4` method synchronizes the execution of jobs - it will wait until all specified jobs not finish. The example output map is presented below:
```json
{
   'msleep2': 'SUCCEED',
   'echo': 'SUCCEED'
}
```

### Examples
The example user program with the QCG PJM API is presnted below:
```python
import zmq

from qcg.appscheduler.api.manager import Manager
from qcg.appscheduler.api.job import Jobs

# switch on debugging (by default in api.log file)
m = Manager( cfg = { 'log_level': 'DEBUG' } )

# get available resources
print("available resources:\n%s\n" % str(m.resources()))

# submit jobs and save their names in 'ids' list
ids = m.submit(Jobs().
        add( name= 'host', exec = '/bin/hostname', args = [ '--fqdn' ], stdout = 'host.stdout' ).
        add( name = 'env', exec = '/bin/env', stdout = 'env.stdout', numCores = { 'exact': 2 } )
        )

# list submited jobs
print("submited jobs:\n%s\n" % str(m.list(ids)))

# wait until submited jobs finish
m.wait4(ids)

# get detailed information about submited and finished jobs
print("jobs details:\n%s\n" % str(m.info(ids)))

```

### Running with QCG
The QCG instance available at Eagle cluster contains registered application `qcg-pm-client`. As a single argument, the application requires the user's Python application. During the execution of user's application, the environment is setup with the required Python compiler (version 3.5) as well as all needed modules (`qcg.appscheduler.api`). The example QCG job description is presented below:

```bash
#QCG note=pjm-client

#QCG host=eagle
#QCG walltime=PT1H
#QCG nodes=1:28

#QCG stage-in-file=api_ex.py
#QCG stage-out-dir=.->eagle.wd.${JOB_ID}

#QCG application=qcg-pm-client
#QCG argument=api_ex.py
```

In this case, program from the file `api_ex.py` will be executed in a QCG PilotJob Manager environemnt.
