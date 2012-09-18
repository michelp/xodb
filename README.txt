
xodb is a Xapian object database for Python.

For information on Xapian, please go to http://xapian.org

Xapian stores information in database records called documents.  xodb
is a library which takes ordinary python objects and converts them
into xapian documents.  This database can then be queried for
documents that match a xapian query language expression.

There are main usage patters in xodb, indexing, and querying.

Indexing
--------

Indexing is acomplished by defining a *Schema* object that describes
how a python object of a certain type is used to generate a xapian
document.  For example, here is 'Department' class that has a name,
and a list of employees::

  class Department(object):

      def __init__(self, name, employees):
          self.name = name
          self.employees = employees


Instaces of this python class could be described using the following
Schema::

  from xodb import Schema, String, Array

  class DepartmentSchema(Schema):

      language = String.using(default="en")
      name = String.using(facet=True)
      employees = Array.of(String.using(facet=True))

The schema tells xodb that the object will have the fields language,
name, and employees.  The first two are strings, and the third is an
array of string.  Note that the object definition above does not
provide a language, in this case, the provided default "en" will be
used.

Now a new xapian database can be created with xodb::

  import xodb

  db = xodb.temp()

Next, tell xodb that the DepartmentSchema describes the Department type::

  db.map(Department, DepartmentSchema)

Now, create some departments::

  housing = Department("housing", ['bob', 'jane'])
  monkeys = Department("monkeys", ['bob', rick])

and add them to the database:

  db.add(housing, monkeys)

Since we are done adding objects, flush the changes to disk so that
other xapian readers can see the new data::

  db.flush()

And that's it, now we can query for the data.

Querying
--------

Data is queried out of xapian using the standard xapian QueryParser
query language syntax.  xodb passes the query you provide, straight
into the query parser, so the xapian documentation is the best source
for exact syntax documentation.

In general however the syntax follows a simple "search engine"
expression language where a user can enter a bunch of terms, or terms
prefixed with a "name:" prefix.  For example:

  assert db.query("name:monkeys").next().name == 'monkeys"
