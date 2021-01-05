# QCG-PilotJob tests

Currently the QCG-PilotJob tests are splitted in two parts:

* the general tests that do not require any external services
* the slurm tests which must be executed in Slurm allocation

The general tests are executed by default in Travis at every commit to the Github repository.Due to the complexity of slurm tests, currently they are executed manually on local machines with usage of docker containers.

## QCG-PilotJob slurm tests

To execute slurm tests, QCG-PilotJob repository includes Giovanni Torres' slurm-docker-cluster (https://github.com/giovtorres/slurm-docker-cluster) docker compose scripts for building Slurm services at `slurm-docker-cluster` subdirectory. The detailed image build instruction is described in README.md file. A few modifications was introduced to original scripts to fix installing of epel release and mounting a QCG-PilotJob source directory in running containers.  
> Note: The `env.sh` file must be modified according to local environment. The
> `QCG_PM_REPO_DIR` variable must point to location of QCG-PilotJob repository
> on filesystem accessible by docker containers. In some environments, e.g.
> Windows with docker-machine the local file system might not have the same
> path in docker-machine's virtual machine.

### Start containers

The brief instruction to build and start slurm docker containers is following:

Enter slurm docker cluster directory:

```console
cd slurm-docker-cluster
```

Build slurm images:

```console
docker build -t slurm-docker-cluster:19.05.1 .
```

Include local environment settings:

```console
source env.sh
```

Start slurm containers in background:

```console
docker-compose up -d
```

Register slurm cluster in slurm database (should be exected only once after build of containers):

```console
bash ./register_cluster.sh
```

To verify a proper mounting of QCG-PilotJob repository in slurm containers we can check if listing generated with following command is similar as content of source directory.

```console
docker exec -it slurmctld ls -l /srv/src
```

### Run tests
To run tests the `run-slurm-docker-tests.sh` bash script from the QCG-PilotJob repository root directory must be called:

```console
bash ./run-slurm-docker-tests.sh
```

This scripts submits a pytest command to slurm scheduler and waits after tests stop executing. The output of `pytest` is placed in `src/slurm-tests-out.txt` file and can be traced during pytests execution.

We can pass pytest arguments to the `run-slurm-docker-tests.sh` script, e.g. for executing only specific test:

```console
bash ./run-slurm-docker-tests.sh -vv -s -k test_resume_simple
```
