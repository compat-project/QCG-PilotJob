from distutils.core import setup

setup(
	name="qcg-pilotjob",

	version="0.8.0",

	author="Piotr Kopta",
	author_email="pkopta@man.poznan.pl",

	packages=["qcg.pilotjob", "qcg.pilotjob.api", "qcg.pilotjob.launcher"],
	package_dir={
		"qcg.pilotjob": "src/qcg/pilotjob",
		"qcg.pilotjob.api": "src/qcg/pilotjob/api",
		"qcg.pilotjob.launcher": "src/qcg/pilotjob/launcher",
	},

	url="http://github.com/vecma-project/QCG-PilotJob",

	description="Manage many jobs inside one allocation",

	install_requires=[
		"zmq",
		"click",
		"prompt_toolkit"
		],

    entry_points = {
        'console_scripts': ['qcg-pm-service=qcg.pilotjob.command_line:service','qcg-pm=qcg.pilotjob.client_cmd:qcgpjm'],
    },
)
