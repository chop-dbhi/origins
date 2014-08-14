#!/bin/bash

# Increase number of concurrent files that can be opened
# Neo4j disk optimization: http://docs.neo4j.org/chunked/stable/server-installation.html#linux-install
ulimit -n 65536

# Symlink volume to data directory if they are not equivalent
if [ "$NEO4J_DATA" != "/neo4j/data" ]; then
    ln -s "$NEO4J_DATA" /neo4j/data
fi

# Setup/migrate database
/neo4j/bin/neo4j-shell -path /neo4j/data/graph.db -file /neo4j-setup.cypher > /dev/null

# Start service
/neo4j/bin/neo4j console
