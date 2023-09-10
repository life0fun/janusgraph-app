#!/bin/bash
# groovy -cp "./*" threaded_loader.groovy

java -cp /opt/janusgraph-utils/libs/*:/opt/janusgraph-utils/libs/janusgraph-utils-0.0.1-SNAPSHOT.jar \
 com.ibm.janusgraph.utils.importer.Lookup /etc/opt/janusgraph/janusgraph.properties \
  $1 $2
