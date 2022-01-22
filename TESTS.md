# QCG-PilotJob tests

Currently the QCG-PilotJob tests are split in two parts:

* the general tests that do not require any external services
* the slurm tests which must be executed in a Slurm allocation

The general tests are executed by default in Travis at every commit to the Github repository. Due to the complexity of the slurm tests, currently they are executed manually on local machines using docker containers. When the tests are executed in slurm allocation, the general tests are actually also conducted.

## QCG-PilotJob general tests

To execute only the general tests with pytest, Python >=3.6 is necessary with the packages listed in `components/core/reqs/requirements.txt` installed. The easiest way is to create a virtual environment:

```console
$ python3.6 -m virtualenv -p python3.6 venv-3.6
$ source venv-3.6/bin/activate
$ pip install -r components/core/reqs/requirements.txt
```

Now we can call pytest (from the root directory of the QCG-PilotJob repository):

```console
PYTHONPATH=components/cmds:components/core:components/executor_api python -m pytest components/
```

## QCG-PilotJob slurm tests

To execute all tests, including slurm tests, the QCG-PilotJob repository includes Giovanni Torres' slurm-docker-cluster (https://github.com/giovtorres/slurm-docker-cluster) docker compose scripts for building Slurm services in the `slurm-docker-cluster` subdirectory. The detailed image build instruction is described in the README.md file. A few modifications were introduced to the original scripts to fix installation of epel release and to mount a QCG-PilotJob source directory in the running containers.

> Note: The `env.sh` file must be modified according to the local environment. The
> `QCG_PM_REPO_DIR` variable must point to the location of QCG-PilotJob repository
> on a filesystem accessible by docker containers. In some environments, e.g.
> Windows with docker-machine the local file system might not have the same
> path in docker-machine's virtual machine.

### Start containers

The brief instruction to build and start the slurm docker containers is as follows:

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

Register slurm cluster in slurm database (should be executed only once after building the containers):

```console
bash ./register_cluster.sh
```

To verify that the QCG-PilotJob repository has been mounted into slurm containers properly, we can check if the listing generated with following command is similar to the content of /components/core directory.

```console
docker exec -it slurmctld ls -l /srv/src
ls -l ../components/core
```

### Run tests

To run the tests the `run-slurm-docker-tests.sh` bash script in the QCG-PilotJob repository root directory must be called from there:

```console
cd ..
bash ./run-slurm-docker-tests.sh
```

This script submits a pytest command to the slurm scheduler and waits until the tests are done. The output of `pytest` can be traced from the host during pytest's execution at `components/core/slurm-tests-out.txt`:

```console
tail -f components/core/slurm-tests-out.txt
```

We can pass pytest arguments to the `run-slurm-docker-tests.sh` script, e.g. for executing only a specific test:

```console
bash ./run-slurm-docker-tests.sh -vv -s -k test_resume_simple
```

Once done, the slurm cluster can be stopped again using docker-compose:

```console
cd slurm-docker-cluster
docker-compose down
```

### Testing with different Slurm versions

A virtual cluster using a different version of Slurm can be built by passing a git identifier to the build command like this:

```console
docker build --build-arg SLURM_TAG=<id> -t slurm-docker-cluster:<version> .
```

Some valid values are `slurm-17.02`, `slurm-17.11`, `slurm-18.08`,
`slurm-19.05`, `slurm-20.02`, `slurm-20.11` and `slurm-21.08`, which will get
you the latest point release of the corresponding major release. Prior to
version 20.02, individual point releases were tagged with tags of the form
`slurm-19-05-5-1`, but that practice seems to have been abandoned.

