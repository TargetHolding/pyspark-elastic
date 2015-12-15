#!/usr/bin/env python

import os

from setuptools import setup, find_packages


basedir = os.path.dirname(os.path.abspath(__file__))
os.chdir(basedir)

def f(*path):
	return open(os.path.join(basedir, *path))

setup(
	name='pyspark_elastic',
	maintainer='Frens Jan Rumph',
	maintainer_email='frens.jan.rumph@target-holding.nl',
	version='0.3.0',
	description='Utilities to asssist in working with Elastic Serach and PySpark.',
	long_description=f('../README.md').read(),
	url='https://github.com/TargetHolding/pyspark-elastic',
	license='Apache License 2.0',

	packages=find_packages(),
	include_package_data=True,

	classifiers=[
		'Development Status :: 2 - Pre-Alpha',
		'Environment :: Other Environment',
		'Framework :: Django',
		'Intended Audience :: Developers',
		'License :: OSI Approved :: Apache Software License',
		'Operating System :: OS Independent',
		'Programming Language :: Python',
		'Programming Language :: Python :: 2',
		'Programming Language :: Python :: 2.7',
		'Topic :: Database',
		'Topic :: Software Development :: Libraries',
		'Topic :: Scientific/Engineering :: Information Analysis',
		'Topic :: Utilities',
	]
)
