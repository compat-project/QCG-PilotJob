# QCG-PilotJob
The QCG Pilot Job service for execution of many computing tasks inside one allocation
=======
# The QCG Pilot Manager v 0.7.1


Author: Piotr Kopta <pkopta@man.poznan.pl>, Tomasz Piontek <piontek@man.poznan.pl>, Bartosz Bosak <bbosak@man.poznan.pl>

Copyright (C) 2017-2020 Poznan Supercomputing and Networking Center


## OVERVIEW
The QCG PilotJob Manager system is designed to schedule and execute many small jobs inside one scheduling system allocation. Direct submission of a large group of jobs to a scheduling system can result in long aggregated time to finish as each single job is scheduled independently and waits in a queue. On the other hand the submission of a group of jobs can be restricted or even forbidden by administrative policies defined on clusters.
One can argue that there are available job array mechanisms in many systems, however the traditional job array mechanism allows to run only bunch of jobs having the same resource requirements while jobs being parts of a multiscale simulation by nature vary in requirements and therefore need more flexible solutions.

From the scheduling system perspective, QCG PilotJob Manager is seen as a single job inside a single user allocation. In the other words, the manager controls an execution of a complex experiment consisting of many jobs on resources reserved for the single job allocation. The manager listens to user's requests and executes commands like submit job, cancel job and report resources usage. In order to manage the resources and jobs the system takes into account both resources availability and mutual dependencies between jobs . Two interfaces are defined to communicate with the system: file-based and network-based. The former one is dedicated and more convenient for a static scenarios when a number of jobs is known in advance to the QCG PilotJob Manager start. The network interface is more general and flexible as it allows to dynamically send new requests and track execution of previously submitted jobs during the run-time. 

To allow user's to test their scenarious, the QCG PilotJob Manager supports *local* execution mode, in which all job's are executed on local machine and doesn't require any scheduling system allocation.

## INSTALLATION
The QCG PilotJob Manager requires Python version >= 3.6.

Optionally the latest version of *pip* package manager and *virtualenv* can be insalled in user's directory by following commands:
```bash
$ curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
$ python3 get-pip.py --user
$ pip install --user virtualenv
```

To create private virtual environment for installed packages, type following commands:
```bash
$ virtualenv venv
$ . venv/bin/activate
```

To install QCG PilotJob Manager directly from github.com into virtual environment, type following command:
```bash
$ pip install --upgrade git+https://github.com/vecma-project/QCG-PilotJob.git
```

## RUN
The QCG Pilot Job Manager module provides wrapper command for running Manager service:
```bash
$ qcg-pm-service --help
usage: qcg-pm-service [-h] [--net] [--net-port NET_PORT]
                  [--net-port-min NET_PORT_MIN] [--net-port-max NET_PORT_MAX]
                  [--file] [--file-path FILE_PATH] [--wd WD]
                  [--envschema ENVSCHEMA] [--resources RESOURCES]
                  [--report-format REPORT_FORMAT] [--report-file REPORT_FILE]
                  [--nodes NODES]
                  [--log {critical,error,warning,info,debug,notset}]
                  [--system-core]

optional arguments:
  -h, --help            show this help message and exit
  --net                 enable network interface
  --net-port NET_PORT   port to listen for network interface (implies --net)
  --net-port-min NET_PORT_MIN
                        minimum port range to listen for network interface if
                        exact port number is not defined (implies --net)
  --net-port-max NET_PORT_MAX
                        maximum port range to listen for network interface if
                        exact port number is not defined (implies --net)
  --file                enable file interface
  --file-path FILE_PATH
                        path to the request file (implies --file)
  --wd WD               working directory for the service
  --envschema ENVSCHEMA
                        job environment schema [auto|slurm]
  --resources RESOURCES
                        source of information about available resources
                        [auto|slurm|local] as well as a method of job
                        execution (through local processes or as a Slurm sub
                        jobs)
  --report-format REPORT_FORMAT
                        format of job report file [text|json]
  --report-file REPORT_FILE
                        name of the job report file
  --nodes NODES         configuration of available resources (implies
                        --resources local)
  --log {critical,error,warning,info,debug,notset}
                        log level
  --system-core         reserve one of the core for the QCG-PJM
```

The same Manager service, can by run directly with the python command:
```bash
$ python -m qcg.appscheduler.service --help
```

### EXAMPLE

```bash
$ mkdir tmpdir
$ cd tmpdir
$ cat <<EOF > jobs.json
[
{
    "request": "submit",
    "jobs": [  {
        "name": "date1",
        "execution": {
          "exec": "/bin/date",
          "stdout": "${jname}.stdout",
          "stderr": "${jname}.stderr"
        },
        "resources": {
          "numCores": {
                "exact": 1
          }
        }
    } ]
},
{
    "request": "control",
    "command": "finishAfterAllTasksDone"
}
]
EOF
$ qcg-pm-service --file --file-path jobs.json
```
In the current directory there should be created a subdirectory '.qcgpjm' with a bunch of files, where the most important are:
* `service.log` - with the manager logs
* `jobs.report` - the report from job execution

The number of available resources discovered by the QCG PJM can be checked with:
```bash
$ grep 'available resources' .qcgpjm/service.log
```

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
The QCG PilotJob Manager module named Executor is responsible for execution and control of jobs by interacting with the cluster resource management system. The current implementation contains three different methods of executing jobs:
 - as a local process - this method is used when QCG PilotJob Manager either has been run outside a Slurm allocation or when parameter '--resources local' has been defined,
 - through internal distributed launcher service - currently used only in Slurm allocation for single core jobs,
 - as a Slurm sub job - the job is submitted to the Slurm to be run in current allocation on scheduled resoureces.
The modular approach allows for relatively easy integration also with other queuing systems. The PilotJob Manager and all the jobs controlled by it are executed in a single allocation. To hide this fact from the individual job and to give it an impression that it is executed directly by the queuing system a set of environment variables, typically set by the queuing system, is overwritten and passed to the job. These variables give the application all typical information about a job it can be interested in, e.g. the amount of assigned resources. In case of parallel application an appropriate machine file is created with a list of resources for each job.

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
  - `modules` (optional) `Array of String` - the list of environment modules that should be loaded before start of the job
  - `venv` (optional) `String` - the path to the virtual environment inside in job should be started
  - `model` (optional) `String` - the model of execution, currently only *threads* explicit model is supported which should be used for OpenMP jobs; if not defined - the default, dedicated to the multi processes execution model is used 
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


## LOCAL MODE
The QCG PilotJob Manager supports *local* mode that is suitable for locally testing executiong scenarious. In normal execution mode, where QCG PilotJob Manager is executed in scheduling system allocation, all jobs are launched with the usage of scheduling system. In the *local* mode, the user itself can define the size of available resources and execute it's scenario on such defined resources without the having access to scheduling system. It's worth remembering that QCG PilotJob Manager doesn't verify the physically available resources, also the executed jobs are not launched with any core/processor affinity. Thus the performance of jobs might not be optimal.

The choice between *allocation* (in scheduling system allocation) or *local* mode is made automatically by the QCG PilotJob Manager during the start. If scheduling system environment will be detected, the *allocation* mode will be chosen. In other case, the local mode will be active, and if resources are not defined by the user, the default number of available cores in the system will be taken.

The command line arguments related to the *local* mode are presented below:
- `--nodes NODES` - the available resources definition; the `NODES` parameter should have format: `[NODE_NAME]:CORES[,[NODE_NAME]:CORES]...`
- `--envschema ENVSCHEMA` - job execution environment; for each job the QCG PilotJob Manager can create environment similar to the SLURM execution environment

### Examples of resources definition
* `--nodes 4` - single node with 4 available cores
* `--nodes n1:2` - single named node with 2 available cores
* `--nodes 4,2,2` - three unnnamed nodes with 8 total cores
* `--nodes n1:4, n2:4, n3:4` - three named nodes with 12 total cores


## ADDITIONAL OUTPUT FILES
QCG PilotJob Manager creates the following files in a working directory:
- `service.log` - containing service logs, very useful to debug service and jobs
 execution
- `jobs.report` - holding information about all finished (with success or
 failure) jobs along with the details on status, scheduled nodes/cores,
 working directory and wall time; the format of this file can be selected by the command argument of the QCG PilotJob Manager; currently the *text* and *json* file formats are supported; the sample content of the *text* format is presented below:
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
QCG PilotJob Manager provides a client side Python API which allows users to dynamically control the instance of the Pilot Job and all its jobs. To avoid communication issues, we assume that the user's program (Application Controller) that refers via API to the QCG PilotJob Manager is run inside an allocation as one of the jobs. The communication is done through the network interface provided by QCG PilotJob Manager. The Python API is provided in the `qcg.appscheduler.api` package.

### Manager
The `qcg.appscheduler.api.Manager` class is the main actor of the API. This class is responsible for the initialisation of the client and handles communication with a single instance of QCG PilotJob Manager. The class provides interface for:
- determining the resources status (the `resources` method)
- job submission (the `submit` method),
- listing all jobs (the `list` method),
- obtaining the current status of jobs (the `status` method),
- obtaining the detailed information about jobs (the `info` method),
- removing jobs (the `remove` method),
- synchronisation of the job execution (the `wait4` method).


#### Initialization
```python
def __init__(address = None, cfg = { })
```

To its proper work, the `Manager` class needs to know the address of the QCG PilotJob Manager instance. In case where the client application is run as one of jobs in QCG PilotJob Manager this address is stored in the `QCG_PM_ZMQ_ADDRESS` environment variable, and is automatically used by the API when it was not provided explicitly.

During the initialisation of the `Manager` class the user can specify some variables that configure behaviour of the class. Currently the following configuration keys are supported:
* `poll_delay` - a delay in seconds between successive updates of the job status in the *wait* method,
* `log_file` - a location of the log file
* `log_level` - a logging level (e.g. `DEBUG`); by default the logging level is set to INFO. Levels allowed and supported so far are: *DEBUG* and *INFO*.


#### `resources`
```python
"""
Raises:
        InternalError - in case of request processing internal error
        ConnectionError - in case of lack of connection to the service or nonzero response code.
"""
def resources()
```

This method returns information about current status of resources available (allocated) in QCG PilotJob Manager. The output format is described in [this section](#resourcesInfo-command)


#### `submit`
```python
    """
    Submit jobs.

    Args:
        jobs (qcg.appscheduler.api.Jobs) - a list of job descriptions

    Returns:
        list - a list of names (identifiers) of submitted jobs

    Raises:
        InternalError - in case of request processing internal error
        ConnectionError - in case of lack of connection to the service or nonzero response code.
    """

	def submit(jobs)
```

Submit a list of jobs. The jobs to be submitted are stored in a `Jobs` object. In case of success the list of names (identifiers) of submitted jobs is returned. The returned list can be used in other methods for example to get actual statuses of jobs or to synchronise their execution.

#### `qcg.appscheduler.api.Jobs`
```python

    """
    Validates and adds a new job description in a simple format to the group of jobs.
    If both arguments are present, they are merged and processed as a single dictionary.

    Args:
        dAttrs (dict) - simple format attributes as a dictionary
        attrs (dict) - simple format attributes as a named arguments

    Raises:
        InvalidJobDescriptionError - in case of non-unique job name or invalid job description
    """
    def add(self, dAttrs = None, **attrs)
```
```python
	
    """
    Validates and adds a new job description in a standard format (acceptable to the QCG PJM) to the group.
    If both arguments are present, they are merged and processed as a single dictionary.

    Args:
        dAttrs (dict) - standard format attributes as a dictionary
        stdAttrs (dict) - standard format attributes as a named arguments

    Raises:
        InvalidJobDescriptionError - in case of non-unique job name or invalid job description

    """
    def addStd(self, dAttrs = None, **stdAttrs)
```
```python
	
    """
    Remove a job from the group.

    Args:
        name (str) - name of the job to be removed

    Raises:
        JobNotDefinedError - in case of lack in a group of a job with the given name
    """
    def remove(self, name)
````

```python
    """
    Read job descriptions (in a standard format acceptable to the QCG-PJM) from a JSON file 

    Args:
        filePath (str) - path to the file containing job descriptions in a standard format

    Raises:
        InvalidJobDescriptionError - in case of invalid job description
    """
    def loadFromFile(self, filePath)
```
```python

    """
    Save job list to a file in a JSON format.

    Args:
        filePath (str) - path to the destination file

    Raises:
        FileError - in case of problems with opening / writing output file.
    """
    def saveToFile(self, filePath)
```
The `Jobs` object holds a list of job descriptions. The names of jobs must be unique.  The jobs can be described in a format presented in [this section](#submit-command), as well as in a simplified one. The main difference between formats is that the simplified one has a more flat structure, where `execution` and `resources` elements have been moved to the top level. The next difference is that the  `dependencies` section, has been simplified and replaced with the `after` element, that contains a list of ancestor jobs.

The simplified job description format contains the following keys:
* `name` (required - `True`, allowed types - `str`) - a job's name,
* `exec` (required - `True`, allowed types - `str`) - a path to the executable,
* `args` (required - `False`, allowed types - `list`, `str`) - an argument or a  list of arguments,
* `stdin` (required - `False`, allowed types - `str`) - a path to the standard input file,
* `stdout` (required - `False`, allowed types - `str`) - a path to the standard output file,
* `stderr` (required - `False`, allowed types - `str`) - a path to the standard error file,
* `wd` (required - `False`, allowed types - `str`) - a path to the working directory,
* `modules` (required - `False`, allowed types - 'list`, `str`) - the list of environment modules that should be loaded before start of the job 
* `numNodes` (required - `False`, allowed types - `dict`) - number of nodes requirements (described in [section](#job-description-format)),
* `venv` (required - `False`, allowed types - `str`) - the path to the virtual environment inside in job should be started
* `model` (required - `False`, allowed types - `str`) - the model of execution, currently only *threads* explicit model is supported which should be used for OpenMP jobs; if not defined - the default, dedicated to the multi processes execution model is used
* `numCores` (required - `False`, allowed types - `dict`) - number of cores requirements (described in [section](#job-description-format)),
* `wt` (required - `False`, allowed types - `str`) - a wall-time specification,
* `iterate` (required - `False`, allowed types - `list`) - a list describing iterations, it should contain two or three elements: `start iteration`, `end iteration` and optionally, `the step iteration`,
* `after` (required - `False`, allowed types - `list`, `str`) - a list of (or single entry) of tasks that should finish before current one starts.


#### `list`
```python
    """
    List all the jobs.
    Return a list of all job names along with their statuses.

    Returns:
        list - list of jobs with additional data in format described in 'listJobs' method in QCG PJM.

    Raises:
        InternalError - in case of request processing internal error
        ConnectionError - in case of lack of connection to the service or nonzero response code.
    """
    def list(self)
```
This method returns a list of all submitted and registered (not yet removed) jobs from the QCG PilotJob Manager instance along with their statuses. The output format is described [in](#listJobs-command).


#### `status`
```python
    """
    Return current statuses of the given jobs.

    Args:
        names (list, str) - list of job names

    Returns:
        list - a list of job statuses in the format described in 'jobStatus' method of QCG PJM.

    Raises:
        InternalError - in case of request processing internal error
        ConnectionError - in case of lack of connection to the service or nonzero response code.
    """
    def status(self, names)
```
This method returns a statuses of the given jobs. The output format is described [in](#jobstatus-command).


#### `info`
```python
    """
    Return detailed information about the given jobs.

    Args:
        names (list, str) - a list of job names

    Returns:
        list - a list of job's detailed information in the format described in 'jobStatus' method of
          QCG PJM.

    Raises:
        InternalError - in case of request processing internal error
        ConnectionError - in case of lack of connection to the service or nonzero response code.
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
        InternalError - in case of request processing internal error
        ConnectionError - in case of lack of connection to the service or nonzero response code.
    """
    def remove(self, names)
```
The `remove` method removes specified jobs from the registry of QCG PilotJob Manager. The output format is described [in](#removejob-command).


#### `wait4`
```python
    """
    Wait until the given jobs finish.
    This method waits until all specified jobs finish its execution (successfully or not).
    The QCG PJM is periodically polled about statuses of not finished jobs. The poll interval (2 sec by
    default) can be changed by defining a 'poll_delay' key with appropriate value (in seconds) in
    configuration of constructor.

    Args:
        names (list, str) - a list of job names

    Returns:
        dict - a map with job names and their terminal statuses

    Raises:
        InternalError - in case of request processing internal error
        ConnectionError - in case of lack of connection to the service or nonzero response code.
    """
    def wait4(self, names)
```
The `wait4` method synchronises the execution of jobs - it waits (blocks) until all specified jobs are completed. The example output returned by the method is presented below:
```json
{
   "msleep2": "SUCCEED",
   "echo": "SUCCEED"
}
```

### Examples
An example user program utilising the QCG PilotJob Manager API is presented below:
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
print("submited jobs:\n%s\n" % str(m.list()))


# wait until submited jobs finish
m.wait4(ids)

# get detailed information about submited and finished jobs
print("jobs details:\n%s\n" % str(m.info(ids)))

```

### Running with QCG
The QCG instance available at the Eagle cluster contains registered application `qcg-pm-client`. As a single argument, the application requires a user's Python application. During the execution of user's application, the environment is setup with the required Python compiler (version 3.5) as well as all needed modules (`qcg.appscheduler.api`). An example QCG job description is presented below:

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

In this case, the program from the file `api_ex.py` will be executed in a QCG PilotJob Manager environment.

## Profiling
To profile execution of sample pilot jobs, the *gcc* compiler has to be installed in the system:
```bash
$ sudo yum install gcc
```

From our experience, the *line_profiler* is quite usefull profiler as it has a line resolution:
```bash
$ pip install line_profiler
```

To run profiling, use the *kernprof* tool distributed with the *line_profiler* package:
```bash
$ cd src
$ export PYTHONPATH=.
$ kernprof -v -l qcg/appscheduler/tests/profile_local_sleep.py
```
With the *-v* argument the statistics will be printed directly to standard output.

## Performance issues
 * For executing OpenMP job's the `model` element in `execution` description should be set to `threads` - this will setup correctly execution environment for the multi-thread jobs.
 * It's recommended to use `--system-core` parameter for workflows that contains many small jobs or bigger allocations (>256 cores). This will reserve a single core in allocation for QCG PilogJob Manager service.
 * The logging level *debug* may cause decrease in performance due to the have usage of file system. 

## Dictionary
* **Scheduling system** - a service that controls and schedules access to the fixed set of computational resources (aka. queuing system, workload manager, resource management system). The current implementation of QCG Pilot Job supports SLURM cluster management and job scheduling system.
* **Job** - a sequential or parallel program with defined resource requirements
* **Job array** - a mechanism that allows to submit a set of jobs with the same resource requirements to the scheduling system at once; commonly used in parameter sweep scenarios
* **Allocation** - a set of resources allocated by the scheduling system for a specific time period; resources assigned to an allocation are static and do not change in time
* **QCG PilotJob Manager** - a service started inside a scheduling system allocation that schedules and controls execution of jobs on the same allocation
* **QCG PilotJob Manager API** - an interface in the form of Python module that provides communication with the QCG PilotJob Manager
* **Application Controller** - a user's program run as one of jobs inside QCG PilotJob Manager that, using the QCG PilotJob Manager API, dynamically submits and synchronizes new jobs
