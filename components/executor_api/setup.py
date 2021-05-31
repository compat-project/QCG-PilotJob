from setuptools import setup

with open("README.md", "r") as fh:
	long_description = fh.read()

setup(
	name="qcg-pilotjob-executor-api",
	version="0.11.2",

	author="Bartosz Bosak",
	author_email="bbosak@man.poznan.pl",

	packages=[
		"qcg.pilotjob.executor_api",
		"qcg.pilotjob.executor_api.templates"
	],

	url="http://github.com/vecma-project/QCG-PilotJob",

	description="The executor-like api for QCG-PilotJob",
	long_description=long_description,
	long_description_content_type="text/markdown",

	install_requires=[
	]
)
