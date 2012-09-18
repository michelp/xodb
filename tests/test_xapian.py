import shutil
import datetime
import xodb
import tempfile
import xapian
from xodb import MultipleValueRangeProcessor
from xodb.exc import PrefixError

from nose.tools import assert_raises


class _TestDatabase(object):

    db_factory = None

    def setup(self):
        self.db = self.db_factory()
        assert self.db.backend.get_doccount() == 0

    def teardown(self):
        self.db.close()
        if self.db.db_path and isinstance(self.db.db_path, basestring):
            shutil.rmtree(self.db.db_path)

    def test_stored_prefixes(self):
        t = tempfile.mkdtemp()
        xdb = xodb.Database(t)
        xdb.backend.set_metadata("_XODB_RP_Foo", "bar")
        xdb.backend.set_metadata("_XODB_BP_Ding", "dong")
        xdb.reopen()

        assert xdb.relevance_prefixes['Foo'] == 'bar'
        assert xdb.boolean_prefixes['Ding'] == 'dong'

    def test_replication_read_only(self):
        t = tempfile.mkdtemp()
        assert_raises(TypeError, xodb.Database,
                      t, writable=True, replicated=True)

    def test_replication_path_only(self):
        db = xapian.inmemory_open()
        assert_raises(TypeError, xodb.Database, db, replicated=True)

    def test_replication_close_open(self):
        class FS(xodb.Schema):
            language = 'en'
            x = xodb.Integer

        class F(object):
            x = 9

        def _writer():
            db = xodb.temp()
            db.map(F, FS)
            return db

        writer = _writer()
        f = F()
        writer.add(f)
        path = writer.db_path
        writer.flush()
        
        reader = xodb.open(path, writable=False, replicated=True)
        assert len(reader) == 1

        g = F()
        writer.add(g)
        assert len(reader) == 1
        writer.flush()
        assert len(reader) == 2

    def test_transactions(self):

        class FS(xodb.Schema):
            language = 'en'
            x = xodb.Integer.named('x')

        class F(object):
            x = 9

        def _writer():
            db = xodb.temp()
            db.map(F, FS)
            return db

        writer = _writer()
        f = F()
        j = F()
        assert not writer.count()
        with writer.transaction() as db:
            db.add(f)
        assert writer.count() == 1

        try:
            with writer.transaction() as db:
                db.add(j)
                raise Exception("boo!")
        except:
            assert writer.count() == 1

        try:
            with writer.transaction() as db:
                del db['XX:9']
                raise Exception("boo!")
        except:
            assert writer.count() == 1

        with writer.transaction() as db:
            del db['XX:9']
        assert writer.count() == 0

    def test_replace_by_id(self):

        class FS(xodb.Schema):
            language = 'en'
            x = xodb.Integer.named('x')

        class F(object):
            x = 9

        def _writer():
            db = xodb.temp()
            db.map(F, FS)
            return db

        writer = _writer()
        f = F()
        f.x = 1
        j = F()
        j.x = 2
        assert not writer.count()
        with writer.transaction() as db:
            db.replace(f, 1)
            db.replace(j, 2)
        assert writer.count() == 2

        assert db[1].termlist().next().term == 'XX:1'
        assert db[2].termlist().next().term == 'XX:2'

        j.x = 3
        with writer.transaction() as db:
            db.replace(j, 2)
        assert db[2].termlist().next().term == 'XX:3'


        with writer.transaction() as db:
            del db[1]
            del db[2]
        assert writer.count() == 0

    def test_stored_values(self):
        db = self.db
        assert db.value_count == 0
        assert db.add_value('foo')
        assert db.value_count == 1
        assert db.add_value('foo')
        assert db.value_count == 1
        assert db.add_value('bar')
        assert db.value_count == 2

    def test_duplicate_prefix_detection(self):
        t = tempfile.mkdtemp()
        xdb = xodb.Database(t)
        xdb.add_boolean_prefix('foo', 'bar')
        xdb.add_prefix('foo', 'bar')
        assert len(xdb.boolean_prefixes) == 1
        assert len(xdb.relevance_prefixes) == 0
        assert_raises(PrefixError, xdb.add_prefix, 
                      'foo', 'bar', ignore_duplicates=False)

    def test_duplicate_prefix_query_warning(self):
        t = tempfile.mkdtemp()
        xdb = xodb.Database(t)
        xdb.backend.set_metadata("_XODB_BP_foo", "FOO")
        xdb.backend.set_metadata("_XODB_RP_foo", "FOO")
        xdb.reopen()
        q = xdb.querify('foo:monkey')
        assert [t for t in q] == ['FOOmonkey']

class TestXapianFile(_TestDatabase):
    db_factory = staticmethod(xodb.temp)


class TestXapianInMem(_TestDatabase):
    db_factory = staticmethod(xodb.inmemory)


def test_value_range_processor():
    vp = MultipleValueRangeProcessor(dict(foo=1, bar=2), str.upper)
    assert vp('foo:abc', 'def') == (1, 'ABC', 'DEF')
    assert vp('bar:news', 'def') == (2, 'NEWS', 'DEF')
    assert vp('bar:', 'def') == (2, '', 'DEF')
    assert vp('bar', 'def') == (xapian.BAD_VALUENO, 'bar', 'def')
    assert vp('baz:foo', 'def') == (xapian.BAD_VALUENO, 'baz:foo', 'def')

    qp = xapian.QueryParser()
    db = xodb.temp()
    qp.set_database(db.backend)
    qp.add_valuerangeprocessor(vp)

    query = qp.parse_query('foo:abc..def')
    assert str(query) == 'Xapian::Query(VALUE_RANGE 1 ABC DEF)'

    query = qp.parse_query('bar:abc..def')
    assert str(query) == 'Xapian::Query(VALUE_RANGE 2 ABC DEF)'

    query = qp.parse_query('bar:3..4')
    assert str(query) == 'Xapian::Query(VALUE_RANGE 2 3 4)'

    assert_raises(xapian.QueryParserError, qp.parse_query, 'baz:abc..def')
