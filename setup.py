from distutils.core import setup

setup(
	name="QCGPilotManager",

	version="0.1",

	author="Piotr Kopta",
	author_email="pkopta@man.poznan.pl",

	#packages=["qcg"],
	packages=["qcg.appscheduler"],

	url="http://dokumentacja",

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


