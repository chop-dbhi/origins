# Origins

[![Build Status](https://travis-ci.org/cbmi/origins.png?branch=master)](https://travis-ci.org/cbmi/origins) [![Coverage Status](https://coveralls.io/repos/cbmi/origins/badge.png)](https://coveralls.io/r/cbmi/origins)

**Origins is a data introspecter.** It provides a uniform structure and access layer for inspecting data elements.

**Use Cases**

- Discovery of related elements
- Query based on relationships

## Usage

Import `origins` and connect to a data source. This example uses a SQLite database that comes with the respository.

```python
import origins
origin = origins.connect('sqlite', path='./tests/data/chinook.sqlite')
```

For a more thorough example, step through the [Origins Introduction example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Origins%2520Introduction.ipynb).

## Backends

Backends are grouped by type. The first section lists the backend name and any dependencies that must be installed for the backend.

One or more **options** can be passed to the backend. **Hierarchy** lists the path from the origin to the elements, for example `database.tables` will access the table nodes. `database.tables['foo'].columns` will access all the columns on the `foo` table. The **Attributes** section lists the attributes for each type that are captured by the backend. Attributes that are not captured by all backends will be denoted.

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
- `schemas` (PostgreSQL only)
- `tables`
- `columns`

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


### Delimited Files

- `delimited`
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Delimited%2520Example.ipynb)
- `csv` - alias for using the `,` delimiter
- `tab` - alias for using the `\t` delimiter

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

### Directory

- `directory`
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Directory%2520Example.ipynb)

**Options**

- `path` - Path to directory

**Hierarchy**

- `directory`
- `files`

### VCF Files

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

#### via API

- `redcap-api` - depends on [PyCap](https://pypi.python.org/pypi/PyCap)
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/REDCap%2520API%2520Example.ipynb)

**Options**

- `url` - API URL
- `token` - API token for the project
- `name` - Name of the project being accessed (this is merely an identifier)

#### via CSV

- `redcap-csv`
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/REDCap%2520CSV%2520Example.ipynb)

**Options**

- `path` - Path to the file

## Implementation

### Design

- The API is dynamic to allow the backend to be exposed with familiar names, while still maintaing a consistent API for generic manipulation.
- The top-level entity (as exposed by the backend) is known as the _origin_.
- Except for the origin, nodes are contained in a _branch_ of the origin. For example, relational databases do not have columns defined directly on the database, but rather on separate _tables_. Therefore each table would be considered a branch containing a subset of all elements (columns) in the database.

### Attribute Names

Standardizing on a set of attribute names makes the API more consistent and portable.

- `*_name` - Bare name of the structure, e.g. `table_name`, `column_name`
- `*_label` - Verbose names of the structure. These are intended to be more readable than the `*_name` version.
- `*_index` - The index of the structure if ordered such as columns in a CSV or Excel file, e.g. `column_index`, `sheet_index`

#### Pending Additions

- `*_type` - The _type_ of the structure. Most applicable elements which define the data type of the data it contains, e.g. `column_type`

## Graph Representation

### Entities

- Origin - entry point of a backend source such as a database or CSV file
- Branch - intermediate containers of elements such as tables and collections
- Element - the data elements themselves

### Relationships

**Operational**

- `BRANCH` (uni) - operational relationship for signifying an entity is a branch of the other
    - Valid for Origin &rarr; Branch, Branch &rarr; Branch and Branch &rarr; Element
- `ORIGIN` (uni) - direct operational relationship between the origin and a containing element
    - Valid for Origin &rarr; Element

**Relatedness**

- `SIBLING` (bi) - two elements that share the same immediate parent either origin or branch
    - Used for knowing which elements can be represented together at a record level (e.g. columns on a table, fields in a document)
    - Valid between elements
    - Implicit relationship by default

- `ALIAS` (bi) - two elements are semantically the same, have the same properties, and share the same underlying data
    - In database terms, this would be a foreign key relationship either explicit or implicit
    - The directionality of the relationship determines which element contains only a subset of the data
    - TODO: It may be better have explicit relationship for each type


## Client Methods/Properties

- `version()` - Returns the backend version if applicable, e.g. the database version.
- `connect(user=None, password=None)` - Connects the client to the backend. If applicable and required, the `user` and `password` may be passed here to for backends that require authentication. Note, on client initialization, these arguments are passed through for establishing the initial connection. This generally does not need to be called explicitly when using the API.
- `disconnect()` - Disconnects the client from the backend. This generally does not need to be called explicitly when using the API.

**Database**

- `qn(name)` - For database backends (or any backend using a query language), this method should be implemented to properly quote `name` for use in the query statement.
- `schemas()` - Returns a list of schema attributes. _*PostgreSQL only_
- `tables([schema_name])` - Returns a list of table attributes.
- `columns([schema_name], table_name)` - Returns a list of column attributes.
- `foreign_keys()` - Returns a list of foreign key relationships.

 _*Note, the `schema` argument only applies to PostgreSQL_
## Examples

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
