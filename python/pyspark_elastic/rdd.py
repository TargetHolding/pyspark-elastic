# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	 http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from pyspark.rdd import RDD
from pyspark.serializers import NoOpSerializer
from pyspark_elastic.types import as_java_object, AttrDict
from pyspark_elastic.util import helper, make_es_config

try:
	from itertools import izip
except ImportError:
	izip = zip


try:
	import cjson as json
	json.loads = json.decode
	json.dumps = json.encode
except ImportError:
	try:
		import ujson as json
	except ImportError:
		import json



class EsRDD(RDD):
	def __init__(self, ctx, resource_read=None, query=None, **kwargs):
		kwargs = make_es_config(kwargs, resource_read=resource_read, query=query)
		kwargs = as_java_object(ctx._gateway, kwargs)
		jrdd = helper(ctx).esJsonRDD(ctx._jsc, kwargs)
		rdd = RDD(jrdd, ctx, NoOpSerializer())

		# read the rdd in batches of two (first key then value / doc)
		def pairwise(iterable):
			iterator = iter(iterable)
			return izip(iterator, iterator)
		kvRdd = rdd.mapPartitions(pairwise, True)

		super(EsRDD, self).__init__(kvRdd._jrdd, ctx)

	def esCount(self):
		return helper(self.ctx).esCount(self._jrdd)

	def loads(self):
		return self.mapValues(lambda v: json.loads(v.decode('utf-8')))

	def loadsObj(self):
		return self.mapValues(lambda v: AttrDict.loads(v.decode('utf-8')))

def saveToEs(rdd, resource_write=None, **kwargs):
	saveJsonToEs(rdd.map(json.dumps), resource_write=resource_write, **kwargs)

def saveJsonToEs(rdd, resource_write=None, **kwargs):
	kwargs = make_es_config(kwargs, resource_write=resource_write)
	kwargs = as_java_object(rdd.ctx._gateway, kwargs)
	helper(rdd.ctx).saveJsonToEs(rdd._jrdd, kwargs)
