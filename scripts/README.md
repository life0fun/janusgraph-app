
# JanusGraph Bulk loading Configuration

set into override/janusgraph/janusgraph-cql-server.properties

# build janusgraph image, though the override is under build/override/janusgraph
```
# cd dev/ciphorama-backend/janusgraph
# make build
# docker images
```

# JanusGraph.addVertex() vs gremlin graph traversal addV().

The general recommendation is to not use Graph.addVertex() as that is not a user focused API - it is meant for graph providers like JanusGraph. Only use the Gremlin language for interacting with you graph and that will give you the widest level of code portability.
https://stackoverflow.com/questions/55926637/why-is-janusgraphs-addvertex-much-slower-than-addv-with-a-graph-traversal

# JanusGraph txn Thread Local

janusgraph txn is thread local. In multi-thread env, need to commit the local tx from gremlin console and
invoke .next() to actually exec the txn.
g.V().drop().iterator()
graph.tx().commit()

# Java BulkLoader
```
java -cp /opt/janusgraph-utils/libs/*:/opt/janusgraph-utils/libs/janusgraph-utils-0.0.1-SNAPSHOT.jar
  com.ibm.janusgraph.utils.importer.BulkLoader /etc/opt/janusgraph/janusgraph.properties /opt/janusgraph-utils/scripts/test_address.csv 1
```


# Perf numbers
JanusGraph JVM /opt/janusgraph/conf/jvm-11.options
  -Xms4096m -Xmx4096m -Xss256k -XX:+UseG1GC -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=1

## Gremlin Console
1. using single thread whole batch
loadNode total_nodes 13959 time(ms): 3907
2. using 4 threads
thread id 3 commit total 2975 nodes, skipped 97 time: 1865
thread id 2 commit total 3249 nodes, skipped 90 time: 1920
thread id 0 commit total 3925 nodes, skipped 171 time: 2133
thread id 1 commit total 3917 nodes, skipped 179 time: 2168

## Java Client to bulkload into eth_test graph

### using test_address.csv and test_transaction.csv
Tx commit time is small, majority is in readline, query existence for dedup, and addVertex.

Multiple threads, each commit a batch, is better than single thread multiple batches.

Thread 0 commit one batch 8193 time: 1010
Thread 0 commit one batch 5765 time: 550
thread 0 finished uploading total 13958 skipped 644 time 27903

1 thread, 30875 edges
Thread 0 commit one batch 8193 time: 993
Thread 0 commit one batch 8193 time: 798
Thread 0 commit one batch 8193 time: 584
Thread 0 commit one batch 6296 time: 450
thread 0 finished uploading edgeds total 30875 skipped 3909 time 31973

8 threads, 30875 edges
Loading address done ! Time: 21815

## large dataset with eth 2016/01/01 - 03/31/2016.
105660 nodes, Loading address done ! Time: 37281
1855138 txns, Loading address done ! Time: 4724346


## large dataset with eth_2016-01-01-2016-12-31_address.csv 4
671025 nodes, Loading address done ! Time: 135795
4,522,122 edges, 3 hours
12,913,013 edges, 8 hours
g.E().count(), takes about 12 min;


# traversal statement
```
java -cp /opt/janusgraph-utils/libs/*:/opt/janusgraph-utils/libs/janusgraph-utils-0.0.1-SNAPSHOT.jar
  com.ibm.janusgraph.utils.importer.Lookup /etc/opt/janusgraph/janusgraph.properties
  0x1171c4Af3B696caA00Aeb65eAfB9BcCa4608ECff 1
```

g.V().has("address_hash", "0x1171c4Af3B696caA00Aeb65eAfB9BcCa4608ECff").outE().inV().count()

g.V().has("address_hash", "").repeat(outE().has("txn_value", gt(1)).inV().dedup().sample(4)).times(2).path()
g.V().has("address_hash", "").repeat(out().has("balance", lt(1)).dedup().sample(4)).times(2).path()

g.V(26460176).properties("tags").value().fold()
g.V(26460176).property("tags", "gaming").iterator()
g.V(26460176).property("tags", "sanction").iterator()
g.V().has("tags", within("phishing", "gaming")).valueMap()
## to drop a property value from the set, use hasValue().drop().iterate().
g.V().has("tags", "gaming").properties("tags").hasValue("gaming").drop().iterate()