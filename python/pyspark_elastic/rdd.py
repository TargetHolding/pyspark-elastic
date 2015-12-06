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

try:
	import cjson as json
	json.loads = json.decode
	json.dumps = json.encode
except ImportError:
	try:
		import ujson as json
	except ImportError:
		import json

from pyspark.rdd import RDD
from pyspark_elastic.types import as_java_object, AttrDict
from pyspark_elastic.util import helper, make_es_config


class EsRDD(RDD):
	def __init__(self, ctx, resource_read=None, query=None, **kwargs):
		kwargs = make_es_config(kwargs, resource_read=resource_read, query=query)
		kwargs = as_java_object(ctx._gateway, kwargs)
		jrdd = helper(ctx).esJsonRDD(ctx._jsc, kwargs)
		super(EsRDD, self).__init__(jrdd, ctx)

	def loads(self, attr_dict=False):
		loads = AttrDict.loads if attr_dict else json.loads
		return self.mapValues(loads)

def saveToEs(rdd, resource_write=None, **kwargs):
	saveJsonToEs(rdd.map(json.dumps), resource_write, **kwargs)

def saveJsonToEs(rdd, resource_write=None, **kwargs):
	kwargs = make_es_config(kwargs, resource_write=resource_write)
	kwargs = as_java_object(rdd.ctx._gateway, kwargs)
	helper(rdd.ctx).saveJsonToEs(rdd._jrdd, kwargs)

def _merge_kwargs(d, **kwargs):
	for k, v in kwargs.items():
		if v:
			d[k] = v
	return d
