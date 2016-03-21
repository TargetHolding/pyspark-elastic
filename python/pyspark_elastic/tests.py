from datetime import datetime
import json
import unittest

from elasticsearch.helpers import bulk
from elasticsearch_dsl import Index
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.document import DocType
from elasticsearch_dsl.field import String
from pyspark.conf import SparkConf
from pyspark.rdd import RDD
from pyspark_elastic.context import EsSparkContext


class PysparkElasticTestCase(unittest.TestCase):
    class TestDoc(DocType):
        title = String()

    @classmethod
    def setUpClass(cls):
        conf = SparkConf()
        conf.set('spark.ui.showConsoleProgress', 'false')
        cls.sc = EsSparkContext(conf=conf.setAppName("PySpark Elastic Test"))

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()


    def setUp(self):
        self.index = index = Index('pyspark_elastic')
        index.settings(number_of_shards=4)
        index.create(ignore=400)

        index.doc_type(self.TestDoc)

        self.resource = self.index._name + '/' + self.TestDoc._doc_type.name

    def tearDown(self):
        self.index.delete()


    def rdd(self, query='', doc_type=None, cache=True, as_json=True, **kwargs):
        doc_type = doc_type or self.TestDoc._doc_type.name
        rdd = self.sc.esRDD(self.index._name + '/' + doc_type, query, **kwargs)
        if as_json:
            rdd = rdd.loads()
        if cache:
            rdd = rdd.cache()
        return rdd



class TestsWithData(PysparkElasticTestCase):
    def setUp(self):
        super(TestsWithData, self).setUp()

        self.docs = [
            self.TestDoc(title='doc-' + str(i))
            for i in range(1000)
        ]

        actions = [d.to_dict(include_meta=True) for d in self.docs]

        inserted, errors = bulk(connections.get_connection(), actions=actions, refresh=True)
        self.assertEqual(inserted, len(actions))
        self.assertEqual(len(errors), 0)



class ReadTests(TestsWithData):
    def test_first(self):
        doc = self.rdd().first()
        self.assertTrue(doc != None)
        self.assertEqual(len(doc), 2)

        k, v = doc
        self.assertIsInstance(k, basestring)
        self.assertIsInstance(v, dict)
        self.assertEqual(len(v), 1)
        self.assertTrue('title' in v)

        title = v['title']
        self.assertIsInstance(title, basestring)

    def test_take(self):
        self.assertEquals(len(self.rdd().take(10)), 10)

    def test_count(self):
        self.assertEquals(self.rdd().count(), len(self.docs))

    def test_read_metadata(self):
        read = self.rdd(
            read_metadata=True,
            read_metadata_field='_meta',
            read_metadata_version=True,
        ).collect()

        for _, doc in read:
            self.assertIn('_meta', doc)
            meta = doc['_meta']
            self.assertIn('_score', meta)
            self.assertIn('_index', meta)
            self.assertIn('_type', meta)
            self.assertIn('_id', meta)
            self.assertIn('_version', meta)

    def test_default_resource(self):
        self.assertEqual(self.rdd(resource=self.resource).count(), len(self.docs))

    # es.index.read.missing.as.empty
    # def test_read_missing_index(self):

    # es.field.read.empty.as.null
    # def test_empty_fields(self):


# class QueryTests(PysparkElasticTestCase):
    # test querying with ?uri_query
    # def test_uri_query(self):

    # test querying with { dsl }
    # def test_dsl_query(self):

    # test querying with an external json file containing the query dsl
    # def test_ext_res_query(self):

    # es.field.read.validate.presence
    # def test_query_check(self):


class WriteTests(PysparkElasticTestCase):
    def setUp(self):
        super(WriteTests, self).setUp()
        self.docs = self.sc.parallelize(xrange(100)).map(lambda i: dict(title='doc-' + str(i)))

    def assertWritten(self, docs=None):
        docs = docs or self.docs
        if isinstance(docs, RDD):
            docs = docs.collect()
        read = self.rdd().collect()
        self.assertEqual(set(str(d[1]['title']) for d in read), set(str(d['title']) for d in docs))
        return read


    def test_save_dicts(self):
        self.docs.saveToEs(self.resource)
        self.assertWritten()

    def test_save_json(self):
        self.docs.map(json.dumps).saveJsonToEs(self.resource)
        self.assertWritten()

    def test_save_binary_json(self):
        self.docs.map(lambda d: json.dumps(d).encode()).saveJsonToEs(self.resource)
        self.assertWritten()

    def test_save_with_id(self):
        self.docs = self.sc.parallelize(xrange(100)).map(
            lambda i: dict(
                id=str(i),
                title='doc-' + str(i),
            )
        )

        self.docs.saveToEs(
            self.index._name + '/' + self.TestDoc._doc_type.name,
            mapping_id='id'
        )

        self.assertWritten()

        written = self.docs.collect()
        read = self.rdd().collectAsMap()

        self.assertEqual(len(written), len(read))
        for doc in written:
            self.assertEqual(str(doc['title']), read[doc['id']]['title'])

#     def test_create(self):
#         pass
#
#     def test_update(self):
#         pass
#
#     def test_upsert(self):
#         pass
#
#     def test_save_with_parent(self):
#         pass
#
#     def test_save_with_version(self):
#         pass
#
#     def test_save_with_routing(self):
#         pass
#
#     def test_save_with_ttl(self):
#         pass
#
#     def test_save_with_timestamp(self):
#         pass
#
#     def test_save_include_fields(self):
#         # es.mapping.include
#         pass
#

    def test_save_exclude_fields(self):
        docs = [
            dict(title='1', body='a'),
            dict(title='2', body='b'),
            dict(title='1', body='c'),
        ]

        self.sc.parallelize(docs).saveToEs(self.resource, mapping_exclude='body')
        read = self.rdd().collect()
        self.assertEqual(len(read), 3)
        for doc in read:
            self.assertNotIn('body', doc)

#     def test_save_with_script(self):
#         # es.update.script
#         # es.update.script.lang
#         # es.update.script.params
#         pass
#
    # TODO
    # def test_autocreate_index(self):
    #     index = Index('pyspark_elastic_non_existing')
    #     index.delete(ignore=404)
    #
    #     def save():
    #         self.docs.saveToEs(index._name + '/doc_type', index_auto_create='no')
    #     self.assertRaises(Exception, save)

    def test_default_resource(self):
        self.docs.saveToEs(resource=self.resource)
        self.assertWritten()

    def test_dynamic_resource(self):
        Index('test-1').delete(ignore=404)
        Index('test-2').delete(ignore=404)

        docs1 = [
            dict(idx='test-1', body='something'),
            dict(idx='test-1', body='else'),
        ]
        docs2 = [
            dict(idx='test-2', body='abra'),
            dict(idx='test-2', body='ca'),
            dict(idx='test-2', body='dabra'),
        ]

        self.sc.parallelize(docs1 + docs2).saveToEs(resource_write='{idx}/docs')
        self.assertEqual(self.sc.esRDD('test-1/docs').count(), 2)
        self.assertEqual(self.sc.esRDD('test-2/docs').count(), 3)

        self.assertEqual(
            set(d['body'] for d in self.sc.esRDD('test-1/docs').loads().collectAsMap().values()),
            set(d['body'] for d in docs1)
        )

    def test_dynamic_resource_timestamp(self):
        Index('test-2015-11').delete(ignore=404)
        Index('test-2015-12').delete(ignore=404)

        docs_nov = [
            dict(timestamp=datetime.fromtimestamp(1448363875).isoformat(), body='Lorem'),
            dict(timestamp=datetime.fromtimestamp(1448363876).isoformat(), body='ipsum'),
            dict(timestamp=datetime.fromtimestamp(1448363877).isoformat(), body='dolor'),
        ]

        docs_dec = [
            dict(timestamp=datetime.fromtimestamp(1449400621).isoformat(), body='fee'),
            dict(timestamp=datetime.fromtimestamp(1449400622).isoformat(), body='fi'),
            dict(timestamp=datetime.fromtimestamp(1449400623).isoformat(), body='fo'),
            dict(timestamp=datetime.fromtimestamp(1449400623).isoformat(), body='fum'),
        ]

        self.sc.parallelize(docs_nov + docs_dec).saveToEs(resource_write='test-{timestamp:YYYY-MM}/docs')
        self.assertEqual(self.sc.esRDD('test-2015-11/docs').count(), 3)
        self.assertEqual(self.sc.esRDD('test-2015-12/docs').count(), 4)

        self.assertEqual(
            set(d['body'] for d in self.sc.esRDD('test-2015-11/docs').loads().collectAsMap().values()),
            set(d['body'] for d in docs_nov)
        )

    # def test_serialization_configuration(self):
    #     # es.batch.size.bytes
    #     # es.batch.size.entries
    #     # es.batch.write.refresh
    #     # es.batch.write.retry.count
    #     # es.batch.write.retry.wait
    #     pass


# class ConfTests(PysparkElasticTestCase):
#
#     def test_timeout(self):
#         # es.http.timeout
#         pass
#
#     def test_retries(self):
#         # es.http.timeout
#         pass
#
#     def test_scroll_keepalive(self):
#         # es.scroll.keepalive
#         pass
#
#     def test_scroll_size(self):
#         # es.scroll.size
#         pass
#
#     def test_task_timeout(self):
#         # es.action.heart.beat.lead
#         pass
#
#
# class SecurityTests(PysparkElasticTestCase):
#     def test_authentication(self):
#         # es.net.http.auth.user
#         # es.net.http.auth.pass
#         pass



if __name__ == '__main__':
    connections.create_connection()
    unittest.main()
    # suite = unittest.TestLoader().loadTestsFromTestCase(PushDownTests)
    # unittest.TextTestRunner().run(suite)
