# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

_helper = None

def helper(ctx):
    global _helper

    if not _helper:
        _helper = ctx._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
            .loadClass("pyspark_elastic.PythonHelper").newInstance()

    return _helper

def make_es_config(d, **kwargs):
    cfg = {}
    add_es_config(cfg, d)
    add_es_config(cfg, kwargs)
    return cfg

def add_es_config(cfg, d):
    for k, v in d.items():
        cfg[make_es_param(k)] = str(v)

def make_es_param(k):
    return 'es.' + k.replace('_', '.')
