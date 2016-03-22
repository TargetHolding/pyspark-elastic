DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

VERSION=`cat version.txt`

PYSPARK_DRIVER_PYTHON=ipython \
	$DIR/lib/spark-1.6.1-bin-hadoop2.6/bin/pyspark \
	--conf spark.es.nodes=localhost \
	--driver-memory 2g \
	--master local[*] \
	--packages TargetHolding/pyspark-elastic:$VERSION
	$@

