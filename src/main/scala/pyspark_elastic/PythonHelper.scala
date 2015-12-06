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
