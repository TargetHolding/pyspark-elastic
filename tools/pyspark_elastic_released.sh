DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

VERSION=`grep version $DIR/build.sbt | sed 's/version := //g' | sed 's/"//g'`

PYSPARK_DRIVER_PYTHON=ipython \
	$DIR/lib/spark-1.5.2-bin-hadoop2.6/bin/pyspark \
	--conf spark.es.nodes=localhost \
	--driver-memory 2g \
	--master local[*] \
	--packages TargetHolding/pyspark-elastic:$VERSION
	$@

