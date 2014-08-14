FROM dockerfile/java

MAINTAINER Byron Ruth <b@devel.io>

# Use standard location
ENV NEO4J_DATA /neo4j/data

# Install dependencies
RUN apt-get -qq update

# Set working directory
WORKDIR /

# Download and install Neo4j
RUN curl --silent "http://dist.neo4j.org/neo4j-community-2.1.3-unix.tar.gz" > neo4j-community-2.1.3-unix.tar.gz
RUN tar zxf neo4j-community-2.1.3-unix.tar.gz
RUN rm neo4j-community-2.1.3-unix.tar.gz
RUN rm -rf neo4j-community-2.1.3/data
RUN mv neo4j-community-2.1.3 neo4j

# Uncomment server address to allow for incoming connections
RUN sed -i 's/#org.neo4j.server.webserver.address/org.neo4j.server.webserver.address/g' neo4j/conf/neo4j-server.properties

# Entrypoint into running the Neo4j server
# Defined as a separate entrypoint because it requires running as a privileged container
# for setting the session ulimit
ADD neo4j-setup.cypher .
ADD entrypoint.sh .
RUN chmod +x entrypoint.sh

EXPOSE 7474

ENTRYPOINT /entrypoint.sh
