package pyspark_elastic;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import pyspark_elastic.pickling.BatchPickle;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class PythonHelper {
	public JavaRDD esRDD(JavaSparkContext sc, String resource, String query, Map<String, String> cfg) {
		cfg = readConfig(resource, query, cfg);
		return JavaEsSpark.esRDD(sc, cfg).mapPartitions(new BatchPickle(), true);
	}

	public JavaRDD esJsonRDD(JavaSparkContext sc, String resource, String query, Map<String, String> cfg) {
		cfg = readConfig(resource, query, cfg);
		return JavaEsSpark.esJsonRDD(sc, cfg).mapPartitions(new BatchPickle(), true);
	}
	
	public void saveJsonToEs(JavaRDD<String> rdd, String resource, Map<String, String> cfg) {
		cfg = writeConfig(resource, cfg);
		JavaEsSpark.saveJsonToEs(rdd, cfg);
	}

	private Map<String, String> readConfig(String resource, String query, Map<String, String> cfg) {
		if (cfg == null) {
			cfg = new HashMap<>();
		}
	
		if (resource != null) {
			cfg.put(ConfigurationOptions.ES_RESOURCE_READ, resource);
		}
	
		if (query != null) {
			cfg.put(ConfigurationOptions.ES_QUERY, query);
		}
		
		return cfg;
	}

	private Map<String, String> writeConfig(String resource, Map<String, String> cfg) {
		if (cfg == null) {
			cfg = new HashMap<>();
		}
	
		if (resource != null) {
			cfg.put(ConfigurationOptions.ES_RESOURCE_WRITE, resource);
		}
		
		return cfg;
	}
}
