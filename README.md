# Origins

[![Build Status](https://travis-ci.org/cbmi/origins.png?branch=master)](https://travis-ci.org/cbmi/origins) [![Coverage Status](https://coveralls.io/repos/cbmi/origins/badge.png)](https://coveralls.io/r/cbmi/origins)

Origins supports extacting structural and descriptive metadata from data resources such as relational databases, document stores, web services, and structured text files.

Origins provides a uniform programmatic interface to accessing metadata from various resources. It removes the need to know how PostgreSQL stores it's schema information, how REDCap data dictionaries are structured, and how get all the fields and their occurrences across documents in a MongoDB collection.

## Quick Usage

Import `origins` and connect to a resource. This example uses a SQLite database that comes with the respository.

```python
>>> import origins
>>> db = origins.connect('sqlite', path='./tests/data/chinook.sqlite')
>>> db.tables
(Table('Album'),
 Table('Artist'),
 Table('Customer'),
 Table('Employee'),
 Table('Genre'),
 Table('Invoice'),
 Table('InvoiceLine'),
 Table('MediaType'),
 Table('Playlist'),
 Table('PlaylistTrack'),
 Table('Track'))

>>> db.tables['Employee'].columns['Title'].props
{'default_value': None,
 'index': 3,
 'name': 'Title',
 'nullable': True,
 'primary_key': 0,
 'type': 'NVARCHAR(30)'}
```

For a more thorough example, walk through the [Origins Introduction example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Origins%2520Introduction.ipynb).

## Goals

*Given an element and it's resource, extract enough metadata to support accessing the underlying data.*

For *physical* resources, this provides a foundation for extraction of additional information such as statistics about the data itself and arbitrary queries. For *logical* resources, the metadata could be used for constructing the data model itself.

Some elements are either explicitly or semantically related, such as being enforced through a referential constraint or a synonym in an ontology. These are generally referred to as *relationships*.

## Outcomes/Use Cases

- General information purposes
    - Registry of resources for a project, team, organization, etc.
    - Cross-organization collaboration
    - Annotate with high-level transformations required to move data out of systems
- Data provenance
    - Includes lineage of data
    - Invalidation of references
        - If A references B and B changes, the reference to B may no longer be valid
- Definition of logical resources for ETL workflows
    - *Shopping cart* of elements across resources that may be required or useful for a project
- "Common Data Elements"
    - Logical resource containing the canonical elements that other physical elements map to across resources
- Normalized data access layer
    - Metadata can be used to generate queries, statements, expressions, web requests, etc. for fetching the underlying data
    - As with any programmatic access layer (e.g. an ORM), the data model needs to be expressed somewhere that can be mapped to the underlying system's interface, i.e. a language (SQL), API or protocol


## Resource Backends

A backend is composed of a *client* and optionally a set of classes for each structural component in the metadata, such as *database*, *table*, and *column* for relationship databases.

The client does all the *heavy lifting* of connecting to the resource and extracting the metadata. If the structural classes are available (all built-in backends have these), the metadata can be accessed and traversed using a simple hiearchy/graph API (see the Quick Usage example above).

### Builtin Backends

Backends are grouped by type. The first section lists the backend name and any dependencies that must be installed for the backend.

One or more **options** can be passed to the backend. **Hierarchy** lists the path from the origin to the elements, for example `db.tables` will access the table nodes. `db.tables['Employee'].columns` will access all the columns on the `Employee` table.

#### Relational Databases

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

#### Document Stores

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

#### Files

- `delimited` - General backend for accessing fixed width delimited files.
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Delimited%2520Example.ipynb)
- `csv` - Alias for the `,` delimiter
- `tab` - Alias for the `\t` delimiter
- `datadict` General backend for data dictionary-style delimited files.
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/DataDict%2520Example.ipynb)

**Options**

- `path` - Path to the file
- `delimiter` - The delimiter between fields, defaults to comma
- `header` - A list/tuple of column names. If not specified, the header will be detected if it exists, otherwise the column names will be the indices.
- `sniff` - The number of bytes to use of the file when detecting the header.
- `dialect` - `csv.Dialect` instance. This will be detected if not specified.

**Hierarchy**

- `file`
- `columns` (delimited) | `fields` (datadict)

#### Excel

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

#### File System

- `directory`
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Directory%2520Example.ipynb)

**Options**

- `path` - Path to directory

**Hierarchy**

- `directory`
- `files`

#### Variant Call Format (VCF) Files

- `vcf` - requires [PyVCF](https://pypi.python.org/pypi/PyVCF)
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/VCF%2520Example.ipynb)

**Options**

- `path` - Path to VCF file

**Hierarchy**

- `file`
- `field`

#### REDCap (via MySQL)

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

#### REDCap (via API)

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

#### REDCap (via CSV data dictionary)

- `redcap-csv`
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/REDCap%2520CSV%2520Example.ipynb)

**Options**

- `path` - Path to the REDCap data dictionary CSV file

**Hierarchy**

- `project`
- `forms`
- `fields`


#### Harvest API

- `harvest` - depends on [requests](https://pypi.python.org/pypi/requests)
    - [Example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Harvest%2520Example.ipynb)

**Options**

- `url` - Harvest API URL
- `token` - Harvest API token if authentication is required

**Hierarchy**

- `application`
- `categories`
- `concepts`
- `fields`


## Resource Exporter

The resource exporter returns a JSON-compatible format representing the nodes and relationships from the "connect" API above. This format is intended to be consumed by the Origins graph API, however it can serve as a general purpose format for other consumers.

See an [example usage](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Resource%20Exporter.ipynb)
