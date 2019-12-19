from distutils.core import setup

setup(
	name='QCGPilotManagerAPI',

	version='0.6.2',

	author='Piotr Kopta',
	author_email='pkopta@man.poznan.pl',

	packages=['qcg.appscheduler.api'],
        package_dir={'qcg.appscheduler.api': 'src/qcg/appscheduler/api'},

	url='http://dokumentacja',

	description='API for QCG PilotJob Manager',

	install_requires=[
		'zmq',
		],
)


