SHELL = /bin/bash
VERSION = $(shell cat version.txt)

.PHONY: clean clean-pyc clean-dist dist test-travis



clean: clean-dist clean-pyc

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-dist:
	rm -rf target
	rm -rf python/build/
	rm -rf python/*.egg-info



install-venv:
	test -d venv || virtualenv venv
	
install-elastic-driver: install-venv
	venv/bin/pip install elasticsearch-dsl
	
install-elastic:
	export URL=https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.1.0/elasticsearch-2.1.0.tar.gz ; \
	mkdir -p lib ; cd lib ; \
	test -d elasticsearch-2.1.0 || curl $$URL | tar xz

start-elastic: install-elastic
	nohup lib/elasticsearch-2.1.0/bin/elasticsearch > elastic.log 2>&1 & echo $$! > elastic.pid
	
stop-elastic:
	kill `cat elastic.pid`



test: test-python test-scala test-integration

test-python:

test-scala:



test-integration: \
	test-integration-setup \
	test-integration-matrix \	
	test-integration-teardown
	
test-integration-setup: \
	start-elastic

test-integration-teardown: \
	stop-elastic
	
test-integration-matrix: \
	install-elastic-driver \
	test-integration-spark-1.4.1 \
	test-integration-spark-1.5.0 \
	test-integration-spark-1.5.1 \
	test-integration-spark-1.5.2

test-travis:
	$(call test-integration-for-version,$$SPARK_VERSION,$$SPARK_PACKAGE_TYPE)

test-integration-spark-1.4.1:
	$(call test-integration-for-version,1.4.1,hadoop2.6)

test-integration-spark-1.5.0:
	$(call test-integration-for-version,1.5.0,hadoop2.6)

test-integration-spark-1.5.1:
	$(call test-integration-for-version,1.5.1,hadoop2.6)

test-integration-spark-1.5.2:
	$(call test-integration-for-version,1.5.2,hadoop2.6)

define test-integration-for-version
	echo ======================================================================
	echo testing integration with spark-$1
	
	mkdir -p lib && test -d lib/spark-$1-bin-$2 || \
		(pushd lib && curl http://ftp.tudelft.nl/apache/spark/spark-$1/spark-$1-bin-$2.tgz | tar xz && popd)
	
	cp log4j.properties lib/spark-$1-bin-$2/conf/

	source venv/bin/activate ; \
		lib/spark-$1-bin-$2/bin/spark-submit \
			--master local[*] \
			--driver-memory 256m \
			--jars target/scala-2.10/pyspark-elastic-assembly-$(VERSION).jar \
			--py-files target/pyspark_elastic-$(VERSION)-py2.7.egg \
			python/pyspark_elastic/tests.py
			
	echo ======================================================================
endef



dist: clean-pyc
	sbt assembly
	cd python ; \
		find . -mindepth 2 -name '*.py' -print | \
		zip ../target/scala-2.10/pyspark-elastic-assembly-$(VERSION).jar -@


all: clean dist


publish: clean
	# use spark packages to create the distribution
	sbt spDist

	# push the python source files into the jar
	cd python ; \
		find . -mindepth 2 -name '*.py' -print | \
		zip ../target/scala-2.10/pyspark-elastic_2.10-$(VERSION).jar -@

	# copy it to the right name, and update the jar in the zip
	cp target/scala-2.10/pyspark-elastic{_2.10,}-$(VERSION).jar
	cd target/scala-2.10 ;\
		zip ../pyspark-elastic-$(VERSION).zip pyspark-elastic-$(VERSION).jar

	# send the package to spark-packages
	spark-package publish -c ".sp-creds.txt" -n "TargetHolding/pyspark-elastic" -v $(VERSION) -f . -z target/pyspark-elastic-$(VERSION).zip

