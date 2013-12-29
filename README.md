# Origins

[![Build Status](https://travis-ci.org/cbmi/origins.png?branch=master)](https://travis-ci.org/cbmi/origins) [![Coverage Status](https://coveralls.io/repos/cbmi/origins/badge.png)](https://coveralls.io/r/cbmi/origins)

**Origins is a data introspecter.** It provides a uniform access layer for inspecting data elements that are defined in data stores.

**Use Cases**

- Discovery of related elements
- Query based on relationships

## Quick Usage

Import `origins` and connect to a data source. This example uses a SQLite database that comes with the respository.

```python
import origins
origin = origins.connect('sqlite', path='./tests/data/chinook.sqlite')
```

For a more thorough example, step through the [Origins Introduction example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Origins%2520Introduction.ipynb).

## Backends

Backends are grouped by type. The first section lists the backend name and any dependencies that must be installed for the backend.

One or more **options** can be passed to the backend. **Hierarchy** lists the path from the origin to the elements, for example `database.tables` will access the table nodes. `database.tables['foo'].columns` will access all the columns on the `foo` table.

### Relational Databases

- `sqlite`
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Origins%2520Introduction.ipynb)
- `postgresql` - requires [psycopg2](https://pypi.python.org/pypi/psycopg2)
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/PostgreSQL%2520Example.ipynb)
- `mysql` - requires [PyMySQL](https://pypi.python.org/pypi/PyMySQL) or [MySQL-python](https://pypi.python.org/pypi/MySQL-python)
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/MySQL%2520Example.ipynb)
- `oracle` - requires [cx_Oracle](https://pypi.python.org/pypi/cx_Oracle)
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Oracle%2520Example.ipynb)

**Options**

- `database` - name of the database
- `host` - host of the server
- `port` - port of the server
- `user` - user for authentication
- `password` - password for authentication

**Hierarchy**

- `database`
- `schemas` (PostgreSQL only)*
- `tables`
- `columns`

*Note: In addition to the PostgreSQL backend supporting schemas, it also provides direct access to the tables under the `public` schema via the `tables` property.*

### Document Stores

- `mongodb` - requires [PyMongo](https://pypi.python.org/pypi/pymongo)
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/MongoDB%2520Example.ipynb)

**Options**

- `database` - name of the database
- `host` - host of the server
- `port` - port of the server
- `user` - user for authentication
- `password` - password for authentication

**Hierarchy**

- `database`
- `collections`
- `fields`*

*Note: the fields of nested documents are not indexed, however this could be implemented as an option if a use case presents itself.*

### Files

- `delimited` - General backed for acessing fixed width delimited files.
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Delimited%2520Example.ipynb)
- `csv` - Alias for the `,` delimiter
- `tab` - Alias for the `\t` delimiter

**Options**

- `path` - Path to the file
- `delimiter` - The delimiter between fields, defaults to comma
- `header` - A list/tuple of column names. If not specified, the header will be detected if it exists, otherwise the column names will be the indices.
- `sniff` - The number of bytes to use of the file when detecting the header.
- `dialect` - `csv.Dialect` instance. This will be detected if not specified.

**Hierarchy**

- `file`
- `columns`

### Excel

- `excel` - requires [openpyxl](https://pypi.python.org/pypi/openpyxl)
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Excel%2520Example.ipynb)

**Options**

- `path` - Path to the file
- `headers` - If `True`, the first row on each sheet will be assume to be the header. If `False` the column indices will be used. If a list/tuple, the columns will apply to the first sheet. If a dict, keys are the sheet names and the values are a list/tuple of column names for the sheet.

**Hierarchy**

- `workbook`
- `sheets`
- `columns`

_Note: Sheets are assumed to be fixed width based on the first row._

### Directory

- `directory`
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Directory%2520Example.ipynb)

**Options**

- `path` - Path to directory

**Hierarchy**

- `directory`
- `files`

### Variant Call Format (VCF) Files

- `vcf` - requires [PyVCF](https://pypi.python.org/pypi/PyVCF)
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/VCF%2520Example.ipynb)

**Options**

- `path` - Path to VCF file

**Hierarchy**

- `file`
- `field`

### REDCap

#### via MySQL

- `redcap-mysql` - depends on MySQL backend
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/REDCap%2520MySQL%2520Example.ipynb)

**Options**

- `project` - name of the project to access
- `database` - name of the database (defaults to 'redcap')
- `host` - host of the server
- `port` - port of the server
- `user` - user for authentication
- `password` - password for authentication

**Hierarchy**

- `project`
- `forms`
- `fields`

#### via API

- `redcap-api` - depends on [PyCap](https://pypi.python.org/pypi/PyCap)
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/REDCap%2520API%2520Example.ipynb)

**Options**

- `url` - REDCap API URL
- `token` - REDCap API token for the project
- `name` - Name of the project being accessed (this is merely an identifier). _Note, this is required since PyCap does not currently export the name of the project itself through it's APIs._

**Hierarchy**

- `project`
- `forms`
- `fields`

#### via CSV

- `redcap-csv`
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/REDCap%2520CSV%2520Example.ipynb)

**Options**

- `path` - Path to the REDCap data dictionary CSV file

**Hierarchy**

- `project`
- `forms`
- `fields`

## Exporting

The data parsed and structured by Origins can be exported to various backends available in the `origins.io` package. The internal [graph](#graph-model) structure makes it very natural to support exporting to graph databases or hierarchical-based structures like JSON or XML.

Due to immediate needs by the project author, the first exporter is for [Neo4j](http://www.neo4j.org) which is an open source graph database. As a quick example, it's as easy as doing:

```python
import origins
db = origins.connect('sqlite', path='chinook.sqlite')
origins.export('neo4j', db)
```

This exports the entire structure of the Chinook test database into Neo4j (as represented in Origins). Read the section [Export to Neo4j](#export-to-neo4j) for steps on how to setup a Neo4j server.

To export specific elements, a single element or a list of elements can be passed into the `export` method:

```python
# Exports the Artist table and it's columns
origins.export('neo4j', db.tables['Artist'])
```

By default relationships that start from any nodes being exported will be traversed recursively (reverse relationships are not). For more fine grain control of the behavior of exporting certain elements, an `Exporter` instance can be created which allows for incrementally preparing nodes and relationships for export.

```python
# Initialize
exporter = origins.exporter('neo4j')

# Prepare some nodes and relationships
exporter.prepare(node1)
exporter.prepare([node2, node3], traverse=False)

# Export to default target
exporter.export()
# Export to another target..
exporter.export(uri='http://localhost:7476/db/data/')
```

---

## Implementation

### Graph Model

The Origins API builds upon a simple and flexible graph data structure located in the `origins.graph` module. As a quick introduction, [graphs](http://en.wikipedia.org/wiki/Graph_(data_structure)) are composed of vertices and arcs, but more commonly referred to as nodes and edges. In this context, however, we are going to refer to edges as "relationships".

Nodes are connected to other nodes via relationships. Relationships and their types are arbitrary which makes a graph structure incredibly flexible, but prone to high complexity. There are [many types of graphs](http://en.wikipedia.org/wiki/Graph_(mathematics)#Types_of_graphs); each of which define their own set of constraints.

The structures defined in `origins.graph` support defining _directed property graphs_ with at most a single relationship between two nodes of the same type (but can have multiple relationships of other types). Both nodes and relationships can have properties which makes it suitable for storing data _about_ the relationship itself.

Origins backend API builds on this foundation and utilizes a _tree graph_ for representing the structure of a backend (located in `origins.backends.base`). For example, a database contains tables each of which contains columns. This is directed and _acyclic_ since descedent nodes never loop back to themselves nor to any ancestor. This parent-child relationship makes it very convenient when incrementally exposing more detail about the structure of the backend.

However, non-structural relationships such as foreign keys, aliases, sibling, etc. require the flexibility of creating arbitrary relationships which is why the more flexible underlying graph API exists.

### Relationship Types

- `CONTAINS` - denotes the target node is contained in the source node, e.g. table X contains column Y.

- `SIBLING` - two elements that share the same immediate parent either origin or branch
    - Used for knowing which elements can be represented together at a record level (e.g. columns on a table, fields in a document)
    - Valid between elements
    - Implicit relationship by default

- `ALIAS` - two elements are semantically the same, have the same properties, and share the same underlying data
    - In database terms, this would be a foreign key relationship either explicit or implicit
    - The directionality of the relationship determines which element contains only a subset of the data
    - TODO: It may be better have explicit relationship for each type

## Implementing a Backend

The two requirements for writing a backend is to define a client class and the _origin_ node class. The backend module must have these names defined for the backend to be used correctly. For example, this is a perfectly valid backend:

```python
from origins.backends import base

class Client(base.Client):
    pass

class Origin(base.Node):
    pass
```

In fact Origins contains this backend:

```python
import origins
node = origins.connect('noop')
```

It does not synchronize with backend, but provides the same API for manually constructing relationships and adding properties if there is a need for that.

### Fetch & Sync

Unlike above, a useful backend requires connecting to a data store, reading a file, or sending web requests in order to fetch data from the backend. When the origin node is initialized, it synchronizes with the backend (via the client) and fetches and stores data as properties and relationships. For example, when the MongoDB backend's database synchronizes, it fetchs a bunch of stats from the backend and sets them as it's initial properties.

```python
db = origins.connect('mongodb', 'chinook')
print(db.props)

{u'avgObjSize': 116.24218400358033,
 u'collections': 13,
 u'dataFileVersion': {u'major': 4, u'minor': 5},
 u'dataSize': 1818144,
 u'db': u'chinook',
 u'fileSize': 201326592,
 u'indexSize': 596848,
 u'indexes': 11,
 u'name': 'chinook',
 u'nsSizeMB': 16,
 u'numExtents': 28,
 u'objects': 15641,
 u'ok': 1.0,
 u'storageSize': 4026368,
 u'version': u'2.4.8'}
```

The `origins.base.Node` base class has `sync` method and calls it when an instance is initialized. Subclasses should override this method to contain the logic for communicating with it's client:

```python
class Database(base.Node):
    def sync(self):
        self.update(self.client.get_database_props())
```

The _initial sync phase_ is also generally the time to initialize and sync related nodes such as intermediate structures including tables, views, scheets, and collections as well as the data elements themselves including fields and columns. One way of this doing is to setup the descendent nodes in `sync`:

```python
class Database(base.Node):
    def sync(self):
        self.update(self.client.get_database_props())

        for props in self.client.get_tables():
            # Ensure to pass the parent and client!
            table = Table(props, parent=self, client=self.client)
            # Create a relationship to table, e.g. database CONTAINS table
            self.relate(table, 'CONTAINS', {'container': 'table'})


class Table(base.Node):
    def sync(self):
        # setup columns..
```

The `sync` method now creates relationships to the tables that are contained in the database. These tables can now be referenced from `database` like this:

```
# Filter on container type in case the database contains of structures
# such as views.
tables = db.rels(type='CONTAINS').filter('containter', 'table').nodes()
```

This looks quite verbose, so we typically wrap this as a property:

```python
class Database(base.Node):
    # ... sync method

    @property
    def tables(self):
        return self.rels(type='CONTAINS').filter('container', 'table').nodes()
```

Now you can simply do `db.tables` to access the underlying tables.

## Example Uses

### Export to Neo4j

#### Download & Setup Neo4j

Download Neo4j (2.0+ required) from the website: http://www.neo4j.org/download. On Mac OS X or Linux, uncompress, `cd` into the directory, then run:

```
./bin/neo4j start
```

to start the server.

_Mac OS X and Linux requires [Java JDK 7.0+](http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html), the Windows installer includes it._

#### Extract metadata using Origins

We will use the chinook SQLite database (which is included in the source code) or download it [here](https://github.com/cbmi/origins/blob/master/tests/data/chinook.sqlite?raw=true):

```python
import origins
from origins.io import neo4j

db = origins.connect('sqlite', path='chinook.sqlite')
neo4j.export(db)
```

#### View the graph

Open up a modern browser to http://localhost:7474/browser/ and enter the following statement and hit enter in the query prompt bar:

```
MATCH (n:Origin) RETURN n
```

This will render a single node (assuming this is your first time doing this) that corresponds to the chinook database. Double-click on the node to expand all the tables within the database and then each table can be double-clicked to expand the columns.

#### Additional Notes

Neo4j has support constraints and indexes. The `uri` property on an Origins node is intended to be a unique since it is constructed relative to the origin node. Although `MERGE` statements are used during export (which ensures no duplicates are created with the same `uri`), Neo4j will happily allow a `CREATE` statement with a node containing the same `uri`. To prevent this, a constraint can be added to the database:

```
CREATE CONSTRAINT ON (n:Origins) ASSERT n.uri IS UNIQUE
```

All nodes exported by Origins have a `Origins` label to differentiate themselves from other nodes in the graph. Using it here will ensure the constraint is only applied to these nodes and not all nodes in the graph.

A secondary benefit to applying a constraint is getting an index on `uri` for free which makes lookups an O(1) operation.


## Random Notes

### Installing cx-Oracle on Mac OS X

_Note: an account on Oracle's website is required to downloaded the client libraries._

Download `instantclient-basic-macos.x64-11.2.0.3.0.zip` and `instantclient-sdk-macos.x64-11.2.0.3.0.zip` from the [Oracle Mac OS downloads page](http://www.oracle.com/technetwork/topics/intel-macsoft-096467.html) (the point version may be different from the one's listed here). Move the two files to the same directory (`/usr/local/lib` is a good spot).

```
cd /usr/local/lib
unzip instantclient-basic-macos.x64-11.2.0.3.0.zip
unzip instantclient-sdk-macos.x64-11.2.0.3.0.zip
cd instantclient_11_2
ln -s libclntsh.dylib.11.1 libclntsh.dylib
```

Put these environment variables exports in your `~/.bash_profile`:

```
export ORACLE_HOME=/usr/local/lib/instantclient_11_2
export LD_LIBRARY_PATH=$ORACLE_HOME:$LD_LIBRARY_PATH
export DYLD_LIBRARY_PATH=$ORACLE_HOME:$DYLD_LIBRARY_PATH
```

Then run `source ~/.bash_profile` to ensure they are set. _Now_ you can install cx-Oracle:

```
pip install cx-Oracle
```
