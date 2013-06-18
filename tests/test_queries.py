# -*- coding: utf-8 -*-

import shutil
import xodb
from datetime import datetime, date

from xodb import (
    Array,
    Date,
    DateTime,
    Dict,
    Integer,
    List,
    Location,
    Schema,
    String,
    Text,
    )


class Person(object):

    def __init__(self, name, last, job,
                 hired, clocked,
                 salary, rank,
                 description, cualificaciones, kw,
                 friends, properties, location):
        self.name = name
        self.last = last
        self.job = job
        self.hired = hired
        self.clocked = clocked
        self.salary = salary
        self.rank = rank
        self.description = description
        self.cualificaciones = cualificaciones
        self.kw = kw
        self.friends = friends
        self.properties = properties
        self.location = location


class PersonSchema(Schema):

    language = String.using(default="en")

    last = String.using(prefix=False)
    name = String.using(sortable=True, facet=True, wdf_inc=2)
    job = String.using(sortable=True, boolean=True)
    department = String.using(facet=True)

    hired = Date.using(sortable=True)
    clocked = DateTime.using(sortable=True)

    salary = Integer.named('salary')
    rank = Integer.using(sortable=True)
    nothing = Integer.using(facet=True, optional=True)
    nostore = Integer.using(optional=True, store=False)

    description = Text.using(prefix=False, sortable=True)
    cualificaciones = Text.using(language="es")

    keywords = Array.of(String)

    @keywords.property
    def _(self, obj, element):
        return obj.kw.split()

    def j_man(self, obj, element):
        return obj.friends + ['jesus']
    friends = List.of(String).using(getter=j_man)

    properties = Dict.of(
        String.using(name='status'),
        Integer.named('shoe_size'),
        )

    location = Location.named('location')


class Department(object):

    def __init__(self, name, employees):
        self.name = name
        self.employees = employees


class DepartmentSchema(Schema):

    language = String.using(default="en")
    name = String.using(facet=True)
    employees = Array.of(String).using(facet=True)


db = None


def setup():
    global db
    db = xodb.temp()
    db.map(Person, PersonSchema)
    db.map(Department, DepartmentSchema)

    joe = Person(name=u'joe', last=u'bob', job='cake inspector',
                 salary=4500, rank=2, hired=date(1999, 9, 9),
                 clocked=datetime(2010, 10, 10, 10, 10),
                 description="I am the eggmans walking",
                 cualificaciones=u"páseador de perros",
                 kw='one two walking shoes',
                 friends=['rick', 'roll'],
                 properties=dict(status='annoying', shoe_size=3),
                 location=(7.0625, -95.677068),
                 )
    joe.nothing = None

    jane = Person(name=u'jane', last=u'bob', job='steak inspector',
                  salary=5600, rank=100, hired=date(2000, 9, 9),
                  clocked=datetime(2011, 11, 11, 11, 11),
                  description="I am the womans modernize",
                  cualificaciones="Devorador des pasteles fritos",
                  kw='three four busting rhymes',
                  friends=['alice', 'aligator'],
                  properties=dict(status='pleasant', shoe_size=99),
                  location=(51.500152, -0.126236),
                  )

    department = Department('housing',
                                 employees=[joe.name,
                                            jane.name])
    department2 = Department('joe',
                                  employees=[joe.name])

    joe.department = department2.name
    jane.department = department.name

    db.add(joe, jane, department, department2)


def teardown():
    if db.db_path:
        shutil.rmtree(db.db_path)


def assert_one(query, name, result, **kw):
    l = list(db.query(query, **kw))
    assert len(l) == 1
    assert getattr(l[0], name) == result


def test_empty_query():
    assert len(list(db.query(''))) == 4


def test_count():
    assert db.count() == len(db) == 4
    assert db.count("bob") == 2
    assert db.count("name:joe") == 2
    assert db.count("name:jane") == 1
    assert db.count("employees:joe") == 2
    assert db.count("employees:jane") == 1
    assert db.count("name:joe OR name:jane") == 3


def test_describe_query():
    assert db.describe_query('foo') == 'Xapian::Query(foo:(pos=1))'


def test_string():
    assert_one("name:jane", 'name', u'jane')
    assert db.count("bob") == 2
    assert db.count("last:jug") == 0
    assert db.count('job:"cake inspector"') == 1
    assert db.count('job:cake') == 0
    assert db.count('job:inspector') == 0
    assert db.count('job:"steak inspector"') == 1

    assert ([t.job for t in
             db.query("bob", order="job")] ==
            ['cake inspector', 'steak inspector'])

    assert ([t.job for t in
             db.query("bob", order="job", reverse=True)] ==
            ['steak inspector', 'cake inspector'])


def test_wdf():
    # this is kind of a low level test, but should hold for all future iterations of wdf algorithm
    assert db.query('name:joe')
    assert ([[(t.term, t.wdf) for t in d._xodb_document.termlist()] for d in db.query('name:joe')] == 
            [[('XCLOCKED:20101010', 1),
              ('XCUALIFICACIONES:de', 1),
              ('XCUALIFICACIONES:paseador', 1),
              ('XCUALIFICACIONES:perros', 1),
              ('XDEPARTMENT:joe', 1),
              ('XFACET:department', 1),
              ('XFACET:name', 1),
              ('XFRIENDS_0:rick', 1),
              ('XFRIENDS_1:roll', 1),
              ('XFRIENDS_2:jesus', 1),
              ('XHIRED:19990909', 1),
              ('XJOB:cake inspector', 1),
              ('XKEYWORDS:one', 1),
              ('XKEYWORDS:shoes', 1),
              ('XKEYWORDS:two', 1),
              ('XKEYWORDS:walking', 1),
              ('XLANGUAGE:en', 1),
              ('XNAME:joe', 2),
              ('XPROPERTIES_SHOE_SIZE:3', 1),
              ('XPROPERTIES_STATUS:annoying', 1),
              ('XRANK:2', 1),
              ('XSALARY:4500', 1),
              ('X_:one', 1),
              ('X_:shoes', 1),
              ('X_:two', 1),
              ('X_:walking', 1),
              ('ZXCUALIFICACIONES:paseador', 1),
              ('ZXCUALIFICACIONES:perr', 1),
              ('Zeggman', 1),
              ('Zwalk', 1),
              ('am', 1),
              ('bob', 1),
              ('eggmans', 1),
              ('i', 1),
              ('loc_watttatcttttgctacgaagt', 1),
              ('the', 1),
              ('walking', 1)],
             # this is the department
             [('XEMPLOYEES:joe', 1),
              ('XFACET:employees', 1),
              ('XFACET:name', 1),
              ('XLANGUAGE:en', 1),
              ('XNAME:joe', 1)]])

def test_integer():
    assert_one("salary:4500", 'salary', 4500)
    assert_one("salary:4500", 'name', u'joe')
    assert_one("salary:5600", 'salary', 5600)
    assert_one("salary:5600", 'name', u'jane')

    assert db.count("salary:0") == 0
    assert db.count("salary:1") == 0

    assert db.count("rank:0..1") == 0
    assert db.count("rank:1..99") == 1
    assert db.count("rank:1..101") == 2
    assert db.count("rank:99..101") == 1
    assert db.count("rank:102..200") == 0

    assert ([t.rank for t in
             db.query("bob", order="rank")] ==
            [2, 100])

    assert ([t.rank for t in
             db.query("bob", order="rank", reverse=True)] ==
            [100, 2])


def test_date():
    assert_one("hired:19990909", 'hired', date(1999, 9, 9))
    assert_one("hired:19990909", 'name', u'joe')
    assert_one("hired:20000909", 'hired', date(2000, 9, 9))
    assert_one("hired:20000909", 'name', u'jane')

    assert ([t.hired for t in
             db.query("bob", order="hired")] ==
            [date(1999, 9, 9), date(2000, 9, 9)])

    assert ([t.hired for t in
             db.query("bob", order="hired", reverse=True)] ==
            [date(2000, 9, 9), date(1999, 9, 9)])


def test_datetime():
    assert_one("clocked:20101010", 'clocked',
                    datetime(2010, 10, 10, 10, 10))
    assert_one("clocked:20101010", 'name', u'joe')
    assert_one("clocked:20111111", 'clocked',
                    datetime(2011, 11, 11, 11, 11))
    assert_one("clocked:20111111", 'name', u'jane')

    assert_one("clocked:20101009..20101010", 'name', u'joe')
    assert_one("clocked:20111111..20111112", 'name', u'jane')

    assert ([t.clocked for t in
             db.query("bob", order="clocked")] ==
            [datetime(2010, 10, 10, 10, 10),
             datetime(2011, 11, 11, 11, 11)])

    assert ([t.clocked for t in
             db.query("bob", order="clocked", reverse=True)] ==
            [datetime(2011, 11, 11, 11, 11),
             datetime(2010, 10, 10, 10, 10)])


def test_text():
    assert_one("eggman", 'name', u'joe', language="en")
    assert_one("woman", 'name', u'jane', language="en")
    assert_one("eggmans", 'name', u'joe', language="en")
    assert_one("womans", 'name', u'jane', language="en")
    assert_one("walk", 'name', u'joe', language="en")
    assert_one("modern", 'name', u'jane', language="en")

    assert_one("cualificaciones:paseador",
                    'name', u'joe', language="es")
    assert_one("cualificaciones:paseador",
               'cualificaciones', u'páseador de perros',
               language="es")
    assert_one("cualificaciones:devorador",
                    'name', u'jane', language="es")
    assert_one("cualificaciones:devor",
                    'name', u'jane', language="es")
    assert_one("cualificaciones:perro",
                    'name', u'joe', language="es")
    assert_one("cualificaciones:frito",
                    'name', u'jane', language="es")


def test_array():
    assert_one("keywords:one", 'name', u'joe')
    assert_one("keywords:three", 'name', u'jane')


def test_list():
    assert_one("friends_0:rick", 'name', u'joe')
    assert_one("friends_1:aligator", 'name', u'jane')
    l = [t.name for t in
         db.query("friends_2:jesus", order="rank")]
    assert l == [u'joe', u'jane']


def test_dict():
    assert_one("properties_status:annoying", 'name', u'joe')
    assert_one("properties_shoe_size:99", 'name', u'jane')
    assert_one("properties_shoe_size:99", 'properties',
                    dict(status='pleasant', shoe_size=99))


def test_location():
    assert_one("loc_watttatcttttgctacgaagt", 'name', u'joe')
    assert_one("loc_wattt*", 'name', u'joe')
    assert_one("loc_wccttcttcttacaacgagagt'", 'name', u'jane')
    assert_one("loc_wcctt**", 'name', u'jane')
    l = [t.name for t in
         db.query("loc_w*", order="rank")]
    assert l == [u'joe', u'jane']


def test_suggest():
    assert (set(db.suggest('bob')) ==
            set([u'walking', u'eggmans', u'modernize', u'womans']))

    assert (set(db.suggest('bob', prefix='name')) ==
            set([u'name:joe', u'name:jane']))

    assert (set(db.suggest('bob', prefix='department')) ==
            set([u'department:housing', u'department:joe']))

    assert (set(db.suggest('name:joe', prefix='department')) ==
            set([u'department:joe']))

    assert (set(db.suggest('name:jane', prefix='department')) ==
            set([u'department:housing']))

    assert (set(db.suggest('name:jane', prefix='department')) ==
            set([u'department:housing']))


def test_facet():
    assert (db.facet('name:housing') ==
            {u'facet:employees': 1, u'facet:name': 1})

    assert (db.facet('name:housing OR name:jane') ==
            {u'facet:employees': 1,
             u'facet:department': 1,
             u'facet:name': 2})

    assert not db.facet('name:housing AND name:jane')

    assert (db.facet('name:joe') ==
            {u'facet:employees': 1,
             u'facet:department': 1,
             u'facet:name': 2})

    assert (db.facet('name:jane') ==
            {u'facet:department': 1, u'facet:name': 1})

    assert (db.facet('name:jane OR name:joe') ==
            {u'facet:department': 2,
             u'facet:name': 3,
             u'facet:employees': 1})

    assert not db.facet('name:jane AND name:joe')

    assert not db.count('facet:nothing')

    assert (db.facet('employees:jane') ==
            {u'facet:employees': 1, u'facet:name': 1})

    assert (db.facet('employees:joe') ==
            {u'facet:employees': 2, u'facet:name': 2})

    assert (db.facet('employees:joe OR employees:jane') ==
            {u'facet:employees': 2, u'facet:name': 2})

    assert (db.facet('employees:joe AND employees:jane') ==
            {u'facet:employees': 1, u'facet:name': 1})

    assert not db.facet('employees:joe AND name:jane')


def test_metadata():
    l = list(db.query('friends_2:jesus'))
    assert len(l) == 2
    assert l[0]._xodb_document
    assert l[1]._xodb_document

    assert l[0]._xodb_percent == 100
    assert l[1]._xodb_percent == 100
    assert l[0]._xodb_rank == 0
    assert l[1]._xodb_rank == 1
    assert l[0]._xodb_weight == 0.0
    assert l[1]._xodb_weight == 0.0

    l = list(db.query('the'))
    assert len(l) == 2
    assert l[0]._xodb_percent == 100
    assert l[1]._xodb_percent == 97
    assert l[0]._xodb_rank == 0
    assert l[1]._xodb_rank == 1
    assert l[0]._xodb_weight > l[1]._xodb_weight
