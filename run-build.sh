#!/bin/bash
#*******************************************************************************
#   Copyright 2017 IBM Corp. All Rights Reserved.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#*******************************************************************************
usage() {
cat <<EOF
Usage: $0 [options]
options:
    gencsv    Generate CSV, schema, datamapper
    import    Import CSV to JanusGraph
    loadsch   Load schema to JanusGraph
EOF
    exit 1
}

pushd . > /dev/null
sDir=$(dirname "$0")
cd "${sDir}" || usage
sDir=$(pwd -P)
popd > /dev/null

# check if the jar exists or not
utilityJar=$(find "${sDir}"/build/libs -name 'janusgraph-utils*.jar' 2>/dev/null | sort -r | head -1)
if [ -z "${utilityJar}" ]; then
    echo "Please run 'mvn package' to build the project first"
    exit 1
fi

if [ -z "$JANUSGRAPH_HOME" ]; then
    JANUSGRAPH_HOME=$sDir
else
    echo "JanusGraph lib path is set to $JANUSGRAPH_HOME/lib"
fi

#CP="${sDir}/build/libs/commons-csv-1.4.jar:${sDir}/build/libs/*:${sDir}/conf:${JANUSGRAPH_HOME}/lib/*"
CP="${sDir}/build/libs/*:"
class=$1
echo "java -cp ${CP}:${utilityJar} com.ibm.janusgraph.utils.importer.BulkLoader $2"
case $class in
    gencsv)
        shift
        java -cp "$CP":"${utilityJar}" com.ibm.janusgraph.utils.generator.JanusGraphBench "$@"
        ;;
    import)
        shift
        java -cp "$CP":"${utilityJar}" com.ibm.janusgraph.utils.importer.BatchImport "$@"
        ;;
    bulkload)
        shift
        java -cp "$CP":"${utilityJar}" com.ibm.janusgraph.utils.importer.BulkLoader "$@"
        ;;
    lookup)
        shift
        java -cp "$CP":"${utilityJar}" com.ibm.janusgraph.utils.importer.Lookup "$@"
        ;;
    loadsch)
        shift
        java -cp "$CP":"${utilityJar}" com.ibm.janusgraph.utils.importer.schema.SchemaLoader "$@"
        ;;
    *)
        usage      # unknown option
        ;;
esac
#java -cp $CP:target/JanusGraphBench-0.0.1-SNAPSHOT.jar com.ibm.janusgraph.utils.importer.BatchImport $@
#java -cp $CP:target/JanusGraphBench-0.0.1-SNAPSHOT.jar com.ibm.janusgraph.utils.importer.schema.SchemaLoader $@
#java -cp $CP:target/JanusGraphBench-0.0.1-SNAPSHOT.jar com.ibm.janusgraph.utils.generator.JanusGraphBench $@

