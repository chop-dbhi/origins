# Origins Provenance

Documentation is located on the [Origins Provenance wiki pages](https://github.com/cbmi/origins/wiki/Provenance)

## Dependencies

- [Neo4j](http://neo4j.org) 2.0.0+ - graph database
- [requests](http://docs.python-requests.org/) 2.2.0+ - Python HTTP library

### Optional Dependencies

- [ProvToolbox](https://github.com/lucmoreau/ProvToolbox/) - Provides a `provconvert` program that converts between PROV representations including PROV-N, PROV-O, PROV-XML, and PROV-JSON. If this is not installed, all PROV representations *must be* in the PROV-JSON format.
    - The download link is on [wiki page](https://github.com/lucmoreau/ProvToolbox/wiki/ProvToolbox-Home)

## Example

This uses the a [PROV Primer example](http://www.w3.org/TR/prov-primer/#the-complete-example) in the PROV-JSON representation:

```python
from origins.provenance import Neo4jWriter, load_document

url = 'https://raw.githubusercontent.com/cbmi/origins/master/origins/provenance/examples/primer.json'

with Neo4jWriter() as writer:
    load_document(url, writer)
```

A `writer` instance is initialized which is the target for loading the document. There are three `Writer` classes implemented, the base `Writer` which writes to stdout, the `StreamWriter` which writes to the supplied file-like interface, and the `Neo4jWriter` which writes to the Neo4j REST API.
- Documents can be local paths or URLs
- If the ProvToolbox dependency is installed, the other formats can be used also and it will be converted automatically into the required format.


Now you can navigate to the Neo4j browser (http://localhost:7474/browser/ by default) and run this query to return the namespaces used by this document.

```
MATCH (n:`origins:Namespace`) RETURN n
```

Double-click on any of the nodes to show related nodes.
