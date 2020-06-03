Installation
============

The QCG PilotJob Manager requires Python version >= 3.6.

Optionally the latest version of *pip* package manager and *virtualenv*
can be insalled in user's directory by following commands:

.. code:: bash

   $ curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
   $ python3 get-pip.py --user
   $ pip install --user virtualenv

To create private virtual environment for installed packages, type
following commands:

.. code:: bash

   $ virtualenv venv
   $ . venv/bin/activate

There are two options for the actual installation of QCG-PilotJob. You can use the PyPi repository
or install the package from GitHub.

PyPi
----
The installation of QCG-PilotJob from the PyPi repository is as simple as:

.. code:: bash

   $ pip install qcg-pilotjob


GitHub
------

To install QCG-PilotJob directly from github.com type the following command:

.. code:: bash

   $ pip install --upgrade git+https://github.com/vecma-project/QCG-PilotJob.git
