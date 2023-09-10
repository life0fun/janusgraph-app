package com.ciphorama.janusgraph.utils.importer;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.id;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.property;

public class Lookup {
    public static int DEFAULT_HOPS = 2;
    public static int SAMPLE_SIZE = 10;

    public static GraphTraversalSource initTraversal(String janusgraphHost) {
        // JanusGraph graph = JanusGraphFactory.open("janusgraph.properties");
        // MessageSerializer<?> serializer = Serializers.GRAPHBINARY_V1D0.simpleInstance();
        MessageSerializer<?> serializer = Serializers.GRAPHSON_V3D0.simpleInstance();
        serializer.configure(Map.of(
            "ioRegistries", List.of("org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry"),
            "serializeResultToString", true),
            Map.of());
        TypeSerializerRegistry typeSerializerRegistry = TypeSerializerRegistry.build()
            .addRegistry(JanusGraphIoRegistry.instance())
            .create();
        Cluster cluster = Cluster.build()
            .addContactPoint(janusgraphHost)
            .port(8182)
            .serializer(serializer)
            // .serializer(new GraphBinaryMessageSerializerV1(typeSerializerRegistry))
            .maxInProcessPerConnection(32)
            .maxSimultaneousUsagePerConnection(32)
            .maxContentLength(10000000)
            .maxWaitForConnection(10)
            .minConnectionPoolSize(8)
            .maxConnectionPoolSize(12)
            .create();
        return AnonymousTraversalSource
            .traversal()
            .withRemote(DriverRemoteConnection.using(cluster));
    }

    static class Route {
        public List<String> addresses;
        public List<String> transactions;

        public Route() {
            this.addresses = new ArrayList<>();
            this.transactions = new ArrayList<>();
        }

        public String toString() {
            if (addresses.size() == 0) return "";
            List<String> output = new ArrayList<>();
            output.add("v[" + addresses.get(0));
            for (int i = 1; i < addresses.size(); i++) {
                output.add(" -e[" + transactions.get(i-1) + "]");
                output.add(" -v[" + addresses.get(i) + "]");
            }
            return output.toString();
        }
    }

    static List<Vertex> GetNextVertex(GraphTraversalSource g, Vertex root) {
        GraphTraversal<Vertex, Vertex> nextNodes = g.V(root.id()).repeat(out().dedup()).times(1);
        GraphTraversal<Vertex, Edge> nextEdges = g.V(root.id()).outE();
        List<Vertex> nexts = new ArrayList<>();
        while (nextNodes.hasNext()) {
            Vertex next = nextNodes.next();
            nexts.add(next);
            // if (traversal.V(next.id()).inE().count().next() > 20 ||
            //      traversal.V(next.id()).outE().count().next() > 20) {
            //     System.out.println("skipping too many edges to node " + next.value("address_hash"));
            //     continue;
            // }
        }
        // System.out.println("one hope size " + nexts.size());
        return nexts;
    }

    static List<Vertex> GetNextVertex(GraphTraversalSource g, Vertex root, int hops) {
        List<Vertex> output = new ArrayList<>();
        output.add(root);
        int start = 0;
        int end = output.size();
        for (int h = 1; h <= hops; h++) {
            for (int i = start; i < end; i++) {
                output.addAll(GetNextVertex(g, output.get(i)));
            }
            start = end;
            end = output.size();
        }
        return output;
    }

    static List<Route> LookupPath(GraphTraversalSource g, String fromAddr, int hops) {
        Vertex fromNode = g.V().has("address_hash", fromAddr).next();
        List<Route> paths = new ArrayList<>();

        // To capture the edge->vertex [[outE()->inV()]...] full segment in path. out() only capture nodes.
        GraphTraversal<Vertex, Path> path = g.V(fromNode.id()).
            repeat(outE().inV().dedup()).times(hops).sample(SAMPLE_SIZE).path();
        while (path.hasNext()) {
            Path p = path.next();
            Route r = new Route();
            System.out.println("Path " + p.toString());
            p.forEach( (ele, lab) -> {
                if (ele instanceof Vertex) {
                    System.out.println(g.V(((Vertex)ele).id()).values("address_hash").next());
                    String node = (String) g.V(((Vertex)ele).id()).values("address_hash").next();
                    r.addresses.add(node);
                } else {
                    String txn = (String) g.E(((Edge)ele).id()).values("txn_hash").next();
                    r.transactions.add(txn);
                }
            });
            paths.add(r);
        }
        return paths;
    }

    public static void main(String args[]) throws Exception {
        if (null == args || args.length < 2) {
            System.err.println(
                    "Usage: Lookup <janusgraph-config-file> " +
                        "from_address hops");
            System.exit(1);
        }
        System.out.println("Lookup opening JanusGraph with conf file." + args[0]);

        GraphTraversalSource g = initTraversal(args[0]);

        String fromAddr = args[1];
        int hops = DEFAULT_HOPS;
        if (args.length > 2) {
            hops = Integer.parseInt(args[2]);
        }
        long start = System.currentTimeMillis();

        List<Route> paths = LookupPath(g, fromAddr, hops);
        for (Route r : paths) {
            System.out.println(r.toString());
        }

        Vertex root = g.V().has("address_hash",  fromAddr).next();
        List<Vertex> nextVertices = GetNextVertex(g, root, hops);
        System.out.println("From: " + fromAddr + " hops: " + hops + " total next vertices " + nextVertices.size());
        for (int i = 0; i < nextVertices.size(); i++) {
            System.out.println(" v[" + g.V(nextVertices.get(i).id()).values("address_hash").next() + "]");
            if (i > 100) break;
        }
        System.out.println("Lookup Time: " + (System.currentTimeMillis() - start));
        System.exit(0);
    }
}
