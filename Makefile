SHELL = /bin/bash

.PHONY: clean clean-pyc clean-dist dist



clean: clean-dist clean-pyc

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-dist:
	rm -rf target
	rm -rf src/main/python/build
	rm -rf src/main/python/*.egg-info


install-venv:
	test -d venv || virtualenv venv
	

dist: dist-python dist-java

dist-python:
	python/setup.py bdist_egg -d ../target

dist-java:
	mvn package


all: clean dist
