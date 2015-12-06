from json import dumps
import time
import unittest

from elasticsearch.helpers import bulk
from elasticsearch_dsl import Index
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.document import DocType
from elasticsearch_dsl.field import String
from pyspark.conf import SparkConf
from pyspark.rdd import RDD
from pyspark_elastic.context import EsSparkContext


connections.create_connection()

class PysparkElasticTestCase(unittest.TestCase):
    class TestDoc(DocType):
        title = String()

    @classmethod
    def setUpClass(cls):
        cls.sc = EsSparkContext(conf=SparkConf().setAppName("PySpark Elastic Test"))

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()


    def setUp(self):
        self.index = index = Index('pyspark_elastic')
        index.settings(number_of_shards=4)
        index.create(ignore=400)

        index.doc_type(self.TestDoc)

    def tearDown(self):
        self.index.delete()


    def rdd(self, query='', doc_type=None, cache=True):
        doc_type = doc_type or self.TestDoc._doc_type.name
        rdd = self.sc.esRDD('pyspark_elastic/' + doc_type, query)
        if cache:
            rdd = rdd.cache()
        return rdd



class ReadTests(PysparkElasticTestCase):
    def setUp(self):
        super(ReadTests, self).setUp()

        self.docs = [
            self.TestDoc(title='doc-' + str(i))
            for i in range(1000)
        ]

        actions = [d.to_dict(include_meta=True) for d in self.docs]

        inserted, errors = bulk(connections.get_connection(), actions=actions)
        self.assertEqual(inserted, len(actions))
        self.assertEqual(len(errors), 0)


    def test_first(self):
        doc = self.rdd().first()
        self.assertTrue(doc != None)
        self.assertEqual(len(doc), 2)

        k, v = doc
        self.assertEqual(type(k), unicode)
        self.assertEqual(type(v), dict)
        self.assertEqual(len(v), 1)
        self.assertTrue('title' in v)

        title = v['title']
        self.assertEqual(type(title), unicode)

    def test_take(self):
        self.assertEquals(len(self.rdd().take(10)), 10)

    def test_count(self):
        self.assertEquals(self.rdd().count(), len(self.docs))

    def test_read_metadata(self):
        pass
        # es.read.metadata.version
        # es.read.metadata.field
        # es.read.metadata.field

    def test_empty_fields(self):
        pass
        # es.index.read.missing.as.empty
        # es.field.read.empty.as.null

    def test_default_resource(self):
        # es.resource
        pass


class QueryTests(PysparkElasticTestCase):
    def test_uri_query(self):
        # test querying with ?uri_query
        pass

    def test_dsl_query(self):
        # test querying with { dsl }
        pass

    def test_ext_res_query(self):
        # test querying with an external json file containing the query dsl
        pass

    def test_query_check(self):
        # es.field.read.validate.presence
        pass


class WriteTests(PysparkElasticTestCase):
    def setUp(self):
        super(WriteTests, self).setUp()
        self.docs = self.sc.parallelize(xrange(100)).map(lambda i: dict(title='doc-' + str(i)))

    def assertWritten(self, docs=None):
        time.sleep(5)
        docs = docs or self.docs
        if isinstance(docs, RDD):
            docs = docs.collect()
        read = self.rdd().collect()
        self.assertEqual(set(str(d[1]['title']) for d in read), set(str(d['title']) for d in docs))


    def test_save_dicts(self):
        self.docs.saveToEs(self.index._name + '/' + self.TestDoc._doc_type.name)
        self.assertWritten()

    def test_save_json(self):
        self.docs.map(dumps).saveJsonToEs(self.index._name + '/' + self.TestDoc._doc_type.name)
        self.assertWritten()

    def test_create(self):
        pass

    def test_update(self):
        pass

    def test_upsert(self):
        pass

    def test_save_with_id(self):
        pass
    #     (self.docs
    #         .zipWithIndex()
    #         # .map(lambda (d, i): dict(id=i, **d))
    #         .map(lambda (d, i): (i, d))
    #         .saveToEs(
    #             self.index._name + '/' + self.TestDoc._doc_type.name,
    #             **{'es.mapping.id':'id'}
    #         )
    #     )
    #     self.assertWritten()

    def test_save_with_parent(self):
        pass

    def test_save_with_version(self):
        pass

    def test_save_with_routing(self):
        pass

    def test_save_with_ttl(self):
        pass

    def test_save_with_timestamp(self):
        pass

    def test_save_include_fields(self):
        # es.mapping.include
        pass

    def test_save_exclude_fields(self):
        # es.mapping.exclude
        pass


    def test_save_with_script(self):
        # es.update.script
        # es.update.script.lang
        # es.update.script.params
        pass

    def test_autocreate_index(self):
        # es.index.auto.create
        pass

    def test_default_resource(self):
        # es.resource
        pass

    def test_dynamic_resource(self):
        # es.resource.write
        # es.resource.write with format {field-name} for dynamic resource
        # the above with pattern in form {@timestamp:YYYY.MM.dd}
        pass

    def test_serialization_configuration(self):
        # es.batch.size.bytes
        # es.batch.size.entries
        # es.batch.write.refresh
        # es.batch.write.retry.count
        # es.batch.write.retry.wait
        pass


class ConfTests(PysparkElasticTestCase):

    def test_timeout(self):
        # es.http.timeout
        pass

    def test_retries(self):
        # es.http.timeout
        pass

    def test_scroll_keepalive(self):
        # es.scroll.keepalive
        pass

    def test_scroll_size(self):
        # es.scroll.size
        pass

    def test_task_timeout(self):
        # es.action.heart.beat.lead
        pass


class SecurityTests(PysparkElasticTestCase):
    def test_authentication(self):
        # es.net.http.auth.user
        # es.net.http.auth.pass
        pass



if __name__ == '__main__':
    unittest.main()
    # suite = unittest.TestLoader().loadTestsFromTestCase(WriteTests)
    # unittest.TextTestRunner().run(suite)
