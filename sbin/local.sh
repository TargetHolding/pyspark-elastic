DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

VERSION=`grep version $DIR/build.sbt | sed 's/version := //g' | sed 's/"//g'`

PYSPARK_DRIVER_PYTHON=ipython \
	$DIR/lib/spark-1.6.1-bin-hadoop2.6/bin/pyspark \
	--conf spark.es.nodes=localhost \
	--driver-memory 2g \
	--master local[*] \
	--jars $DIR/target/scala-2.10/pyspark-elastic-assembly-$VERSION.jar \
	--py-files $DIR/target/pyspark_elastic-$VERSION-py2.7.egg \
	$@
