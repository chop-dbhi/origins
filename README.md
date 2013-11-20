# Origins

Origins is introspection, indexer, and semantic analyzer of data elements. It connects to data sources and extracts the data elements from those sources and gathers as much metadata about the data as it can an along the way. This metadata is used to show the inherent structure of the elements, but can be used for comparison against elements in other sources.

## Usage

```python
>>> import origins
>>> db = origins.connect('sqlite://./tests/data/chinook.db')
>>> db.elements()
```

Read through a [Origins Introduction example](http://nbviewer.ipython.org/urls/raw.github.com/cbmi/origins/master/notebooks/Origins%2520Introduction.ipynb?token=515142__eyJzY29wZSI6IlJhd0Jsb2I6Y2JtaS9vcmlnaW5zL21hc3Rlci9ub3RlYm9va3MvT3JpZ2lucyBJbnRyb2R1Y3Rpb24uaXB5bmIiLCJleHBpcmVzIjoxMzg1NTU3Nzc0fQ%3D%3D--412f3de08be68e89e61417492787965c1880098a)

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
