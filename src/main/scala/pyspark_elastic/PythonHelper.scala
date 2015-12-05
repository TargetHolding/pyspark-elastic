package pyspark_elastic

import java.util.{ Map => JMap }
import scala.collection.JavaConversions._
import scala.collection.mutable
import org.apache.spark.api.java._
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark
import pyspark_util.Conversions._
import pyspark_util.Pickling._
import java.util.HashMap
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.{ ES_RESOURCE_WRITE, ES_RESOURCE_READ, ES_QUERY }
import pyspark_util.Pickling
import org.apache.spark.rdd.RDD

class PythonHelper() {

  Pickling.register()

  def esJsonRDD(sc: JavaSparkContext, resource: String, query: String, cfg: JMap[String, String]) = {
    val rc = readConfig(resource, query, cfg)
    JavaRDD.fromRDD(sc.sc.esJsonRDD(rc).pickle())
  }

  def saveJsonToEs(rdd: JavaRDD[Array[Byte]], resource: String, cfg: JMap[String, String]) = {
    val wc = writeConfig(resource, cfg)
    rdd.rdd.unpickle().asInstanceOf[RDD[String]].saveJsonToEs(wc)
  }

  def readConfig(resource: String, query: String, cfg: JMap[String, String]) = {
    mutable.Map[String, String]() + (ES_RESOURCE_READ -> resource) + (ES_QUERY -> query) ++ nonEmpty(cfg)
  }

  def writeConfig(resource: String, cfg: JMap[String, String]) = {
    mutable.Map[String, String]() + (ES_RESOURCE_WRITE -> resource) ++ nonEmpty(cfg)
  }

  private[this] def nonEmpty(cfg: JMap[String, String]) = {
    if (cfg == null) {
      mapAsScalaMap(cfg)
    } else {
      Map[String, String]()
    }
  }
}
