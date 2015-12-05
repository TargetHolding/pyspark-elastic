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

PySpark Elastic is tested to be compatible with Spark 1.4 and 1.5. Feedback on (in-)compatibility is much appreciated.


Using with PySpark
------------------

### With Spark Packages
PySpark Elastic is published at [Spark Packages](http://spark-packages.org/package/TargetHolding/pyspark-elastic). This allows easy usage with Spark through:
```bash
spark-submit \
	--packages TargetHolding/pyspark-elastic:<version> \
	--conf spark.es.nodes=your,elastic,node,names
```


### Without Spark Packages

```bash
spark-submit \
	--jars /path/to/pyspark_elastic-<version>.jar \
	--driver-class-path  /path/to/pyspark_elastic-<version>.jar \
	--py-files target/pyspark_elastic_<version>-<python version>.egg \
	--conf spark.es.nodes=your,elastic,node,names \
	--master spark://spark-master:7077 \
	yourscript.py
```
(note that the the --driver-class-path due to [SPARK-5185](https://issues.apache.org/jira/browse/SPARK-5185))



Using with PySpark shell
------------------------

Replace `spark-submit` with `pyspark` to start the interactive shell and don't provide a script as argument and then import PySpark Cassandra. Note that when performing this import the `sc` variable in pyspark is augmented with the `esRDD(...)` and `esJsonRDD(...)` methods.

```python
import pyspark_elastic
```



Building
--------
### For [Spark Packages](http://spark-packages.org/package/TargetHolding/pyspark-elastic) Pyspark Elastic can be compiled using:
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

### For local testing / without Spark Packages
A Java / JVM library as well as a python library is required to use PySpark Elastic. They can be built with:

```bash
make dist
```

This creates 1) a fat jar with the Elasticsearch Hadoop library and additional classes for bridging Spark and PySpark for Elastic Search data and 2) a python source distribution at:

* `target/scala-2.10/pyspark-elastic-assembly-<version>.jar`
* `target/pyspark_elastic_<version>-<python version>.egg`.



API
---

The PySpark Elastic API aims to stay close to the Java / Scala APIs provided by Elastic Search. Reading its [documentation](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html) is a good place to start.


### pyspark_elastic.EsSparkContext

A `CassandraSparkContext` is very similar to a regular `SparkContext`. It is created in the same way, can be used to read files, parallelize local data, broadcast a variable, etc. See the [Spark Programming Guide](https://spark.apache.org/docs/1.2.0/programming-guide.html) for more details. *But* it exposes additional methods:

* ``esRDD(resource, query, **kwargs)``:	Returns a CassandraRDD for the given keyspace and table. Additional arguments which can be provided:

  * `resource` is the index and document type seperated by a forward slash (/)
  * `query` is the query string to apply in searching Elastic Search for data for in the RDD

### pyspark.RDD

PySpark Cassandra supports saving arbitrary RDD's to Cassandra using:

* ``rdd.saveToEs(resource, **kwargs)``: Saves an RDD to resource (which is a / separated index and document type) by dumping the RDD elements using ``json.dumps``.
* ``rdd.saveJsonToEs(resource, **kwargs)``: Saves an RDD to resource (which is a / separated index and document type) directly. The RDD must contain strings.



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
	.set("spark.es.host", "elastic-1")

sc = EsSparkContext(conf=conf)
```

Reading from an index as JSON strings:

```python	
rdd = sc.esJsonRDD('test/tweets')
rdd...
```

Reading from an index as deserialized JSON (dicts, lists, etc.):

```python	
rdd = sc.esRDD('test/tweets')
rdd...
```

Storing data in Elastic Search:

```python
rdd = sc.parallelize([
	{ 'title': x, 'body', x }
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
