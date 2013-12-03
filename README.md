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
- `mysql` - requires [MySQL-python](https://pypi.python.org/pypi/MySQL-python)
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

#### Directory

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
