package com.ciphorama.janusgraph.utils.importer;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.property;

public class UpdateTag {
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
            // .serializer(new GraphSONMessageSerializerV3d0())
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

    public static void main(String args[]) throws Exception {
        if (null == args || args.length < 3) {
            System.err.println(
                "Usage: UpdateTag janusgraph_host " +
                    "address list/add/drop tag");
            System.exit(1);
        }
        System.out.println("UpdateTag opening JanusGraph with conf file. " + args[0]);

        GraphTraversalSource g = initTraversal(args[0]);
        String fromAddr = args[1];
        String action = args[2];
        String tag = "";
        if (args.length > 3) {
            tag = args[3];
        }
        long start = System.currentTimeMillis();
        Vertex root = g.V().has("address_hash",  fromAddr).next();
        System.out.println("updateTag " + fromAddr + " Vertex " + root.id());

        if (action.equalsIgnoreCase("add")) {
            Transaction tx = g.tx();
            GraphTraversalSource gtx = tx.begin();
            g.V(root.id()).property("tags", tag).iterate();
            System.out.println("adding tag " + tag);
            tx.commit();
        } else if (action.equalsIgnoreCase("drop")) {
            Transaction tx = g.tx();
            GraphTraversalSource gtx = tx.begin();
            GraphTraversal<Vertex, ? extends Property<Object>> tagDrop = g.V(root.id()).
                properties("tags").hasValue(tag).drop().iterate();
            System.out.println("dropping tag " + tag);
            tx.commit();
        } else {
            GraphTraversal<Vertex, List<Object>> tags = g.V(root.id())
                .values("tags").fold();
            for(Object s : tags.next()) {
                System.out.println("tag: " + s);
            }
        }
        System.exit(0);
    }
}
