from distutils.core import setup

setup(
	name="QCGPilotManager",

	version="0.4.1",

	author="Piotr Kopta",
	author_email="pkopta@man.poznan.pl",

	packages=["qcg.appscheduler", "qcg.appscheduler.api"],
        package_dir={"qcg.appscheduler": "src/qcg/appscheduler", "qcg.appscheduler.api": "src/qcg/appscheduler/api"},

	url="http://github.com/vecma-project/QCG-PilotJob",

	description="Manage many jobs inside one allocation",

	install_requires=[
		"zmq",
		"click",
		"prompt_toolkit"
		],

    entry_points = {
        'console_scripts': ['qcg-pm-service=qcg.appscheduler.command_line:service'],
    },
)
