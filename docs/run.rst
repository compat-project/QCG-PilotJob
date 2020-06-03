Run
===

The QCG Pilot Job Manager module provides wrapper command for running
Manager service:

.. code:: bash

   $ qcg-pm-service --help
   usage: qcg-pm-service [-h] [--net] [--net-port NET_PORT]
                         [--net-port-min NET_PORT_MIN]
                         [--net-port-max NET_PORT_MAX] [--file]
                         [--file-path FILE_PATH] [--wd WD]
                         [--envschema ENVSCHEMA] [--resources RESOURCES]
                         [--report-format REPORT_FORMAT]
                         [--report-file REPORT_FILE] [--nodes NODES]
                         [--log {critical,error,warning,info,debug,notset}]
                         [--system-core] [--disable-nl] [--show-progress]
                         [--governor] [--parent PARENT] [--id ID] [--tags TAGS]
                         [--slurm-partition-nodes SLURM_PARTITION_NODES]
                         [--slurm-limit-nodes-range-begin SLURM_LIMIT_NODES_RANGE_BEGIN]
                         [--slurm-limit-nodes-range-end SLURM_LIMIT_NODES_RANGE_END]

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
     --disable-nl          disable custom launching method
     --show-progress       print information about executing tasks
     --governor            run manager in the governor mode, where jobs will be
                           scheduled to execute to the dependant managers
     --parent PARENT       address of the parent manager, current instance will
                           receive jobs from the parent manaqger
     --id ID               optional manager instance identifier - will be
                           generated automatically when not defined
     --tags TAGS           optional manager instance tags separated by commas
     --slurm-partition-nodes SLURM_PARTITION_NODES
                           split Slurm allocation by given number of nodes, where
                           each group will be controlled by separate manager
                           (implies --governor)
     --slurm-limit-nodes-range-begin SLURM_LIMIT_NODES_RANGE_BEGIN
                           limit Slurm allocation to specified range of nodes
                           (starting node)
     --slurm-limit-nodes-range-end SLURM_LIMIT_NODES_RANGE_END
                           limit Slurm allocation to specified range of nodes
                           (ending node)

The same Manager service, can by run directly with the python command:

.. code:: bash

   $ python -m qcg.appscheduler.service --help

Example
-------

.. code:: bash

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
   $ qcg-pm-service --file-path jobs.json

In the current directory there should be created a subdirectory with
prefix '.qcgpjm-service' with a bunch of files, where the most important
are:

-  ``service.log`` - with the manager logs
-  ``jobs.report`` - the report from job execution

The number of available resources discovered by the QCG PJM can be
checked with:

.. code:: bash

   $ grep 'available resources:' .qcgpjm/service.log