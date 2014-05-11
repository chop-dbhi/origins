#!/bin/sh

DIR="neo4j-community-2.0.3"
FILE="$DIR-unix.tar.gz"

wget "http://dist.neo4j.org/$FILE"
tar zxf $FILE
$DIR/bin/neo4j start
sleep 3
