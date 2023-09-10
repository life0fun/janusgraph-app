import org.janusgraph.graphdb.management.JanusGraphManager

// graphManager = new org.janusgraph.graphdb.management.JanusGraphManager()
//
// def graph = JanusGraphFactory.open('/opt/janusgraph/conf/eth_test.properties')
def createEthTestSchema(graph) {
    // Get a reference to the management system
    mgmt = graph.openManagement()

    // Define the address vertex label
    address = mgmt.makeVertexLabel('address').make()

    // Define the properties for the address vertex
    addressHash = mgmt.makePropertyKey('address_hash').dataType(String.class).make()
    balance = mgmt.makePropertyKey('balance').dataType(Double.class).make()
    props = mgmt.makePropertyKey('props').dataType(String.class).make()

    // Define tags property with cardinality.List
    tags = mgmt.makePropertyKey('tags').cardinality(Cardinality.SET).dataType(String.class).make()

    // Define the transaction edge label
    transaction = mgmt.makeEdgeLabel('transaction').make()

    // Define the properties for the transaction edge
    txnHash = mgmt.makePropertyKey('txn_hash').dataType(String.class).make()
    blockTimestamp = mgmt.makePropertyKey('block_timestamp').dataType(String.class).make()
    txnValue = mgmt.makePropertyKey('txn_value').dataType(Double.class).make()

    // Define the indexes for efficient querying
    mgmt.buildIndex('byAddressHash', Vertex.class).addKey(addressHash).unique().buildCompositeIndex()
    mgmt.buildIndex('byTxnHash', Edge.class).addKey(txnHash).buildCompositeIndex()
    mgmt.buildIndex('byTags', Vertex.class).addKey(tags).unique().buildCompositeIndex()

    // Commit the schema
    mgmt.commit()
}

// // Connect to JanusGraph
// graph = JanusGraphFactory.open('/opt/janusgraph/conf/janusgraph-cql-es-server.properties')
// graph.close()
// Load addresses from csv
def loadNodes(graph, filename) {
    def start = System.currentTimeMillis()
    def total_nodes = 0
    new File(filename).readLines().findAll { line -> !line.startsWith("address") }.each { line ->
      def parts = line.split(',')
      def address = parts[0]
      def balance = parts.size() > 1 ? parts[1] : null
      def props = parts.size() > 2 ? parts[2] : null
//       def v = graph.addV('address').property('address_hash', address)
      def existingVertex = graph.traversal().V().has('address', 'address_hash', address).tryNext()
      if (existingVertex) {
          println("Skipping duplicate entry for address: ${address}")
          return
      }
      def v = graph.addVertex(T.label, 'address', 'address_hash', address)
      if (balance) {
          v.property('balance', Double.parseDouble(balance))
      }
      if (props) {
          v.property('props', props)
      }
      total_nodes++
    }
    graph.tx().commit()
    def elaps = System.currentTimeMillis() - start
    println("loadNode total_nodes $total_nodes time(ms): $elaps")
}

/*
bugs, TODO: FIXME
Edge Label Name                | Directed    | Unidirected | Multiplicity                         |
---------------------------------------------------------------------------------------------------
transaction                    | true        | false       | MULTI
*/
// Load transactions from csv
def loadEdges(graph, filename) {
    g = graph.traversal()
    new File(filename).readLines().findAll { line -> !line.startsWith("from") }.each { line ->
        def (from, to, txn_hash, block_timestamp, txn_value) = line.split(',')
        def fromVertex = g.V().hasLabel('address').has('address_hash', from).tryNext().orElseGet { g.addV('address').property('address_hash', from).next() }
        def toVertex = g.V().hasLabel('address').has('address_hash', to).tryNext().orElseGet { g.addV('address').property('address_hash', to).next() }
        def existingEdge = g.V(fromVertex).outE('transaction').has('txn_hash', txn_hash).tryNext().orElse(null)
        if (existingEdge) {
            println("Skipping duplicate entry for txn_hash: ${txn_hash}")
            return
        }
        g.addE('transaction')
          .from(fromVertex)
          .to(toVertex)
          .property('txn_hash', txn_hash)
          .property('block_timestamp', block_timestamp)
          .property('txn_value', Double.parseDouble(txn_value).toLong())
          .iterate()
    }
    graph.tx().commit()
}

def createIndexAddressId(graph) {
    def mgmt = graph.openManagement()

    if (!mgmt.containsGraphIndex('addressById')) {
        def idKey = mgmt.containsPropertyKey('address') ? mgmt.getPropertyKey('address') : mgmt.makePropertyKey('address').dataType(String.class).make()
        def addressLabel = mgmt.containsVertexLabel('address') ? mgmt.getVertexLabel('address') : mgmt.makeVertexLabel('address').make()
        mgmt.buildIndex('addressById', Vertex.class).addKey(idKey).indexOnly(addressLabel).buildCompositeIndex()
        mgmt.commit()
    } else {
        println "Index 'addressById' already exists"
    }
}

def reindexAddressId(graph) {
//     JanusGraphManagement mgmt = graph.openManagement()
    def mgmt = graph.openManagement()
    def index = mgmt.getGraphIndex('addressById')
//     enable index
    mgmt.updateIndex(index, SchemaAction.ENABLE_INDEX).get()
    mgmt.commit()

//     JanusGraphIndex index = mgmt.getGraphIndex("addressById")
//     mgmt.updateIndex(index, SchemaAction.REINDEX).get()
//     mgmt.commit()
}

def indexStatus () {
    def graph = JanusGraphFactory.open('/opt/janusgraph/conf/janusgraph-cql-es-server.properties')
    def mgmt = graph.openManagement()
    def index = mgmt.getGraphIndex('addressById')

    if (index instanceof JanusGraphIndex) {
        def indexStatus = mgmt.getIndexStatus(index)
        println "Index status: " + indexStatus
    } else {
        println "Index not found"
    }
    graph.close()
}