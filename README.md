PySpark Elastic
=================

[![Build Status](https://travis-ci.org/TargetHolding/pyspark-elastic.svg)](https://travis-ci.org/TargetHolding/pyspark-elastic)

PySpark Elastic provides python support for Apache Spark's Resillient Distributed Datasets from Elastic Search documents using [Elasticsearch Hadoop](https://www.elastic.co/products/hadoop) within PySpark, both in the interactive shell and in python programmes submitted with spark-submit.

**Contents:**
* [Compatibility](#compatibility)
* [Using with PySpark](#using-with-pyspark)
* [Using with PySpark shell](#using-with-pyspark-shell)
* [Building](#building)
* [API](#api)
* [Examples](#examples)
* [Problems / ideas?](#problems--ideas)
* [Contributing](#contributing)



Compatibility
-------------

TODO



Using with PySpark
------------------

### With Spark 1.3
TODO


### With Spark 1.2 / without Spark Packages

TODO


Using with PySpark shell
------------------------

TODO



Building
--------
### For Spark 1.3 with [Spark Packages](http://spark-packages.org/package/TargetHolding/pyspark-elastic)
Pyspark Elastic can be compiled using:
```bash
sbt compile
```
The package can be published locally with:
```bash
sbt spPublishLocal
```
The package can be published to Spark Packages with (requires authentication and authorization):
```bash
sbt spPublish
```

### For local testing / pre Spark 1.3 versions / without Spark Packages
A Java / JVM library as well as a python library is required to use PySpark Elastic. They can be built with:

```bash
make dist
```

This creates 1) a fat jar with the Elasticsearch Hadoop library and additional classes for bridging Spark and PySpark for Elastic Search data and 2) a python source distribution at:

* `target/pyspark_elastic-<version>.jar`
* `target/pyspark_elastic_<version>-<python version>.egg`.



API
---

TODO

### pyspark_elastic.EsSparkContext

TODO

### pyspark.RDD

TODO

* ``rdd.saveToEs(resource, **kwargs)``: TODO
* ``rdd.saveJsonToEs(resource, **kwargs)``: TODO


### pyspark_elastic.EsJsonRDD

TODO 


### pyspark_elastic.streaming

Not yet implemented


Examples
--------

Creating a SparkContext with Elastic Search support

```python
from pyspark_elastic import EsSparkContext

conf = SparkConf() \
	.setAppName("PySpark Elastic Test") \
	.setMaster("spark://spark-master:7077") \
	.set("es.host", "cas-1")

sc = EsSparkContext(conf=conf)
```

Reading from an index as JSON strings:

```python	
sc.esJsonRDD('test/tweets')
```

Reading from an index as deserialized JSON (dicts, lists, etc.):

```python	
sc.esRDD('test/tweets')
```

Storing data in Elastic Search:

```python
rdd = sc.parallelize([
	{ 'x':x }
	for x in ['a', 'b', 'c']
])

rdd.saveToEs('test/docs')
```



Problems / ideas?
-----------------
Feel free to use the issue tracker propose new functionality and / or report bugs.



Contributing
------------

1. Fork it
2. Create your feature branch (git checkout -b my-new-feature)
3. Commit your changes (git commit -am 'Add some feature')
4. Push to the branch (git push origin my-new-feature)
5. Create new Pull Request
