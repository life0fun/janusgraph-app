
# A standalone Java App to access JanusGraph.

```
git clone 
cd janusgraph-utils/
./gradlew build
```

# Deploy JanusGraph cluster with Scylla DB as backend
Roughly following https://developer.ibm.com/learningpaths/kubernetes-operators/develop-deploy-advanced-operator-janusgraph/develop-janusgraph-operator-cassandra/
to deploy a Janusgraph cluster in AWS. 

Deploy a Scylla DB cluster in the same node. Change janusgraph backend to use Scylla DB.
Create `eth_test` keyspace in scylla db and set janusgraph.properties 
```
storage.hostname =scylla 
storage.cql.keyspace = eth_test`
```
Refer to the `scripts/override` directory properties files for janusgraph and scylla db properties.

# Create schema 
SSH into janusgraph pod, assuming janusgraph $HOME=/opt/janusgraph, 

copy `scripts/eth_test_demo.groovy` into /opt/granusgraph folder.
Load the eth_test_demo.groovy and create the schema.
```
#/opt/janusgraph $ bin/gremlin.sh
:remote connect tinkerpop.server conf/remote.yaml session
:rem console

gremlin> . /opt/janusgraph/eth_test_demo.groovy
gremlin> createEthTestSchema(graph)
```

# JanusGraph Bulk loading Configuration

# Java BulkLoader to load wallets and transactions
After gradlew build, you can run the jar file to load wallets and transaction.

```
java -cp /opt/janusgraph-utils/libs/*:/opt/janusgraph-utils/libs/janusgraph-utils-0.0.1-SNAPSHOT.jar
  com.ciphorama.janusgraph.utils.importer.BulkLoader 
  localhost
  /opt/janusgraph-utils/scripts/eth_2016-01-01-2016-12-31_address.csv 4
  
java -cp /opt/janusgraph-utils/libs/*:/opt/janusgraph-utils/libs/janusgraph-utils-0.0.1-SNAPSHOT.jar
  com.ciphorama.janusgraph.utils.importer.BulkLoader
  localhost 
  /opt/janusgraph-utils/scripts/eth_2016-01-01-2016-12-31_transaction.csv 4
```


# build janusgraph docker image, though the override is under build/override/janusgraph
```
# cd dev/ciphorama-backend/janusgraph
# make build
# docker images
```

## JanusGraph.addVertex() vs gremlin graph traversal addV().

The general recommendation is to not use Graph.addVertex() as that is not a user focused API.
The Graph APIs are meant for graph providers like JanusGraph. 
Client shall only use the Gremlin language for interacting with you graph.
https://stackoverflow.com/questions/55926637/why-is-janusgraphs-addvertex-much-slower-than-addv-with-a-graph-traversal

## JanusGraph txn Thread Local

janusgraph txn is thread local. In multi-thread env, need to commit the local tx from gremlin console and
invoke .next() to actually exec the txn.
g.V().drop().iterator()
graph.tx().commit()

To create a connection to JanusGraph, need to use Cluster and set proper Serializer. see code

See "Traversal Transactions" section in https://tinkerpop.apache.org/docs/3.7.0/reference/.
## JanusGraph Serializer
see Serialization section in Reference doc. https://tinkerpop.apache.org/docs/3.7.0/reference/.


# Increase JanusGraph server JVM mem size. 
JanusGraph JVM /opt/janusgraph/conf/jvm-11.options
  -Xms4096m -Xmx4096m -Xss256k -XX:+UseG1GC -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=1

# Traverse
```
java -cp /opt/janusgraph-utils/libs/*:/opt/janusgraph-utils/libs/janusgraph-utils-0.0.1-SNAPSHOT.jar
  com.ciphorama.janusgraph.utils.importer.Lookup
  localhost  
  0x1171c4Af3B696caA00Aeb65eAfB9BcCa4608ECff 1
```

Fan-out: 2 hops=16k, 3 hops=64k, 4 hops=169k
g.V().has("address_hash", "").repeat(outE().has("txn_value", gt(0)).inV().dedup()).times(3).count()

g.V().has("address_hash", "").repeat(outE().has("txn_value", gt(1)).inV().dedup().sample(4)).times(2).path()
g.V().has("address_hash", "").repeat(out().has("balance", lt(1)).dedup().sample(4)).times(2).path()

g.V(26460176).properties("tags").value().fold()
g.V(26460176).property("tags", "gaming").iterator()
g.V(26460176).property("tags", "sanction").iterator()
g.V().has("tags", within("phishing", "gaming")).valueMap()

To drop a property value from the set, use hasValue().drop().iterate().
g.V().has("tags", "gaming").properties("tags").hasValue("gaming").drop().iterate()


# UpdateTag
```
java -cp /opt/janusgraph-utils/libs/*:/opt/janusgraph-utils/libs/janusgraph-utils-0.0.1-SNAPSHOT.jar
  com.ciphorama.janusgraph.utils.importer.UpdateTag
  localhost
  0x1171c4Af3B696caA00Aeb65eAfB9BcCa4608ECff add/drop/list <tag>
```

# Links
* [Demo on Youtube](https://www.youtube.com/watch?v=1TQcPWgPvF8): Watch the video.

