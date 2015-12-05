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


    # TODO
    # def test_save_with_id(self):
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


if __name__ == '__main__':
    unittest.main()
    # suite = unittest.TestLoader().loadTestsFromTestCase(WriteTests)
    # unittest.TextTestRunner().run(suite)
