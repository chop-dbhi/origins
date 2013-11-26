# Origins

[![Build Status](https://travis-ci.org/cbmi/origins.png?branch=master)](https://travis-ci.org/cbmi/origins) [![Coverage Status](https://coveralls.io/repos/cbmi/origins/badge.png)](https://coveralls.io/r/cbmi/origins)

Origins is introspection, indexer, and semantic analyzer of data elements. It connects to data sources and extracts the data elements from those sources and gathers as much metadata about the data as it can an along the way. This metadata is used to show the inherent structure of the elements, but can be used for comparison against elements in other sources.

## Usage

```python
>>> import origins
>>> db = origins.connect('sqlite', path='./tests/data/chinook.db')
>>> db.elements()
```

Read through a [Origins Introduction example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Origins%2520Introduction.ipynb)

## Backends

Backends are grouped by type and are broken down into subsections. The first section lists the backend name and a list of dependencies that must be installed for the backend. The **Options** section lists all the options that can be passed when loading/connecting to the backend. Options that do not apply to all backends of a given type will be denoted (and are simply ignored). **Hierarchy** lists the path from the origin to the elements, for example `database.tables` will access the table nodes. `database.tables['foo'].columns` will access all the columns on the `foo` table. The **Attributes** section lists the attributes for each type that are captured by the backend. Attributes that are not captured by all backends will be denoted.

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


### Files

- `delimited`
- `csv` (alias to delimited)
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/CSV%2520Example.ipynb)

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

## Design

- The core focus is exposing _just enough_ API for indexing data elements.
- The API is dynamic to allow the backend to be exposed with familiar names, while still maintaing a consistent API for generic manipulation.
- The top-level source (as exposed by the backend) is known as the _origin_.
- Except for the origin, nodes are contained in a _named branch_ which is accessible via the branch's source. For example, the relational database backends expose the `tables` branch on `Database` (the origin) which contain `Table` instances (which are themselves sources).


## Structures

### Origin

Named subclass of a source specifically for use as the top-level (the origin) of the elements. This is the entry point of all descedent sources. Examples include a database, file, directory, HTTP endpoint.

The options used to instantiate the origin should be included in `attrs` since it further describes origin and how it was accessed.

### Source

A source directly or indirectly exposes elements. Examples include the columns of a database table, the fields of a document (in a document store), and the header of a comma-separate values (CSV) text file.

The only requirement of a source is to implement the `elements` method which returns descendent elements.

It is encouraged to define a property that serves as a named proxy for accessing the elements. For example, when working with an relational database backend, accessing the `elements` using a more familiar term such as `columns` would make working with the API more friendly.

For backends that contain a hierarchy, sources can be defined to be nested. For example, a relational database (the origin and source) contains tables (nested sources) each of which contain columns (the elements). Although all elements can be accessed through the origin, exposing the nested sources provides more granular access to the elements. These nested sources are referred to as _branches_ of the current source.

### Node

An element contains all the metadata about data at a specific location relative to it's origin. The contents of the data is not assumed, but in general an element is expected to have a type (whether primtivie or complex) and point to values that can be accessed. For example, a database table or CSV header column name the values contained in the rows.

## TODO

- provide ability to mark attributes from not being stored nor included in any output (such as passwords)
