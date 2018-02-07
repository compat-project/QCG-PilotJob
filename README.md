# The QCG Pilot Manager


Author: Piotr Kopta <pkopta@man.poznan.pl>, Tomasz Piontek <piontek@man.poznan.pl>, Bartosz Bosak <bbosak@man.poznan.pl>

Copyright (C) 2017-2018 Poznan Supercomputing and Networking Center


## OVERVIEW
The QCG PilotJob Manager system is designed to schedule and execute many small tasks inside one scheduling system allocation. Direct submission of a large group of jobs to a scheduling system can result in long aggregated time to finish as each single job is scheduled independently and waits in a queue. On the other hand the submission of a group of tasks can be restricted or even forbidden by administrative policies defined on clusters.
One can argue that there are available job array mechanisms in many systems, however the traditional job array mechanism allows to run only bunch of jobs having the same resource requirements while tasks being parts of a multiscale simulation by nature vary in requirements and therefore need more flexible solutions.

From the scheduling system perspective, QCG PilotJob Manager is seen as a single job inside a single user allocation. In the other words, the manager controls an execution of a complex experiment consisting of many tasks on resources reserved for the single job allocation. The manager listens to user's requests and executes commands like submit job, cancel job and report resources usage. In order to manage the resources and jobs the system takes into account both resources availability and mutual dependencies between tasks . Two interfaces are defined to communicate with the system: file-based and network-based. The former one is dedicated and more convenient for a static pilot job when a number of jobs is known in advance to the QCG PilotJob Manager start. The network interface is more general and flexible as it allows to dynamically send new requests and track execution of previously submitted jobs during the run-time. **Currently only the file interface is implemented and is an subject of validation and verification.**

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
- `resources` (required) `Dict` - resource requirements, a dictionary with the following keys: 
  - `numCores` (optional) `Dict` - number of cores,
  - `numNodes` (optional) `Dict`- number of nodes,
    The specification of `numCores`/`numNodes` elements may contain the following keys:
    - `exact` (optional) `Number` - the exact number of cores,
    - `min` (optional) `Number` - minimal number of cores,
    - `max` (optional) `Number` - maximal number of cores,
    (the `exact` and `min` /  `max` are mutually exclusive)

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

#### jobStatus Command
Report current status of a given job. The `jobName` key must contain a name of a job for which status should be reported. A job may be in one of the following states:
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
    "jobName": "msleep2"
  }
```

The example response is presented below:
```json
{
  "data": {
    "history": "\n2018-01-12 14:58:49.513096: QUEUED\n2018-01-12 14:58:49.514081: EXECUTING",
    "status": "EXECUTING",
    "runtime": {
      "allocation": "e0145:2"
    },
    "jobName": "msleep2"
  },
  "code": 0
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
    "jobName": "msleep2"
}
```

#### listJobs Command
Return a list of registered jobs. No additional arguments are needed.
The example list jobs request is presented below:
```json
{
	"request": "listJobs"
}

The example response is presented below:
```json
{
  "data": {
    "length": 2,
    "names": [
      "echo",
      "msleep2"
    ]
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
The QCG PilotJob Manager service is executed as a regular job in a queuing system. It exploits a single system allocation for itself and all jobs it controls. For the user convenience the service has been registered as QCG application (currently only on cluster Eagle @ PSNC) under the name `qcg-pm`. The only and required argument for this application is the name (path) of the requests file. The example script (in QCG Simple description format) is presented below:
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
