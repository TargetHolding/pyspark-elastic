package pyspark_elastic

import java.util.{ Map => JMap }

import scala.collection.JavaConversions.mapAsScalaMap

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark.sparkContextFunctions
import org.elasticsearch.spark.sparkStringJsonRDDFunctions

import pyspark_util.Pickling
import pyspark_util.Pickling.toPickleableRDD
import pyspark_util.Pickling.toUnpickleableRDD

class PythonHelper() {

  Pickling.register()

  def esJsonRDD(sc: JavaSparkContext, cfg: JMap[String, String]) = {
    JavaRDD.fromRDD(sc.sc.esJsonRDD(config(cfg)).pickle())
  }

  def saveJsonToEs(rdd: JavaRDD[Array[Byte]], cfg: JMap[String, String]) = {
    rdd.rdd.unpickle().asInstanceOf[RDD[String]].saveJsonToEs(config(cfg))
  }

  private[this] def config(cfg: JMap[String, String]) = {
    if (cfg != null) {
      mapAsScalaMap(cfg)
    } else {
      Map[String, String]()
    }
  }
}
