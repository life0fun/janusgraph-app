package com.ciphorama.janusgraph.utils.importer;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import com.ibm.janusgraph.utils.importer.dataloader.DataLoader;
import com.ibm.janusgraph.utils.importer.schema.SchemaLoader;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BulkLoader {
    public static int PER_THREAD_BATCH_SIZE = 1024;

    // see "Traversal Transactions" section in https://tinkerpop.apache.org/docs/3.7.0/reference/.
    public static GraphTraversalSource initTraversal(String janusgraphHost) {
        // JanusGraph graph = JanusGraphFactory.open("janusgraph.properties");
        // the current JanusGraph serializer by default send back GRAPHSON. using graphbinary will get array negative index ex.
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

    public static int commitBatch(JanusGraphTransaction graphTransaction, int threadId, int batch) {
        try {
            long start = System.currentTimeMillis();
            graphTransaction.commit();
            System.out.println("Thread " + threadId + " commit one batch " + batch +
                " time: " + (System.currentTimeMillis() - start));
            return batch;
        } catch (Exception e) {
            System.out.println("Thread " + threadId + " commit one batch " + batch + " Exception " + e);
        }
        return 0;
    }
    public static int commitBatch(Transaction tx, int threadId, int batch) {
        try {
            long start = System.currentTimeMillis();
            tx.commit();
            System.out.println("Thread " + threadId + " commit one batch " + batch +
                " time: " + (System.currentTimeMillis() - start));
            return batch;
        } catch (Exception e) {
            System.out.println("Thread " + threadId + " commit one batch " + batch + " Exception " + e);
        }
        return 0;
    }

    static class AddressWorker implements Runnable {
        String csvFile;
        int threadId;
        int totalThreads;

        GraphTraversalSource g;
        Transaction tx;
        GraphTraversalSource gtx;

        public AddressWorker(GraphTraversalSource g, String csvFile, int threadId, int totalThreads) {
            this.csvFile = csvFile;
            this.threadId = threadId;
            this.totalThreads = totalThreads;
            this.g = g;
            this.tx = g.tx();
            this.gtx = tx.begin();
        }

        @Override
        public void run() {
            int lineNo = 0;
            int batchNodes = 0;
            int skippedNodes = 0;
            int totalNodes = 0;
            long start = System.currentTimeMillis();
            try {
                BufferedReader reader = new BufferedReader(new FileReader(csvFile));
                System.out.println("Thread " + threadId + " Opening Vertex file " + csvFile);

                while (true) {
                    String line = reader.readLine();
                    if (line == null) {
                        System.out.println("Thread " + threadId + " Finished reading vertex ");
                        break;
                    }
                    lineNo++;
                    String[] parts = line.split(",");
                    String address = parts[0];
                    if (address != null && address.length() > 1 &&
                        address.charAt(address.length()-1) % totalThreads == threadId) {
                        String balance = parts.length > 1 ? parts[1] : null;
                        String props = parts.length > 2 ? parts[2] : null;
                        String tags = parts.length > 3 ? parts[3] : null;
                        if (g.V().has("address", "address_hash", address)
                            .tryNext().isPresent()) {
                            skippedNodes++;
                            System.out.println("skipping one node " + address);
                            continue;
                        }
                        try {
                            GraphTraversal<Vertex, Vertex> v = gtx.addV("address");
                            v.property("address_hash", address);
                            if (balance != null) {
                                v.property("balance", Double.parseDouble(balance));
                            }
                            if (props != null) {
                                v.property("props", props);
                            }
                            if (tags != null) {
                                v.property("tags", tags);
                            }
                            v.iterate();
                        } catch (Exception e) {
                            System.out.println("Thread " + threadId + " add address property ex: " + e.toString());
                            skippedNodes++;
                            break;
                        }
                        batchNodes++;
                        if (batchNodes > PER_THREAD_BATCH_SIZE) {
                            totalNodes += commitBatch(tx, threadId, batchNodes);
                            batchNodes = 0;
                            tx = g.tx();
                            gtx = tx.begin();
                        }
                    }
                }
                if (batchNodes > 0) {
                    totalNodes += commitBatch(tx, threadId, batchNodes);
                    batchNodes = 0;
                    tx = g.tx();
                    gtx = tx.begin();
                }
            } catch (Exception e) {
                System.out.println("Thread " + threadId + " exception " + e.toString());
            } finally {
                if (batchNodes > 0) {
                    totalNodes += commitBatch(tx, threadId, batchNodes);
                }
                System.out.println("thread " + threadId + " finished uploading total " + totalNodes +
                    " skipped " + skippedNodes + " time " + (System.currentTimeMillis() - start));
            }
        }
    }

    static class EdgeWorker implements Runnable {
        String csvFile;
        int threadId;
        int totalThreads;

        GraphTraversalSource g;
        Transaction tx;
        GraphTraversalSource gtx;

        public EdgeWorker(GraphTraversalSource g, String csvFile, int threadId, int totalThreads) {
            this.csvFile = csvFile;
            this.threadId = threadId;
            this.totalThreads = totalThreads;
            this.g = g;
            this.tx = g.tx();
            this.gtx = tx.begin();
        }

        @Override
        public void run() {
            int lineNo = 0;
            int batchEdges = 0;
            int skippedEdges = 0;
            int totalEdges = 0;
            long start = System.currentTimeMillis();

            try {
                BufferedReader reader = new BufferedReader(new FileReader(csvFile));
                System.out.println("Thread " + threadId + " Opening Vertex file " + csvFile );

                while (true) {
                    String line = reader.readLine();
                    if (line == null) {
                        System.out.println("Thread " + threadId + " Finished reading txn " + lineNo);
                        break;
                    }
                    lineNo++;
                    String[] parts = line.split(",");
                    if (parts.length < 5) { continue; }
                    String from = parts[0];
                    String to = parts[1];
                    String txnHash = parts[2];
                    String blockTimestamp = parts[3];
                    double txnValue = Double.parseDouble(parts[4]);

                    if (from.length() > 1 && (int)(from.charAt(from.length() - 1)) % totalThreads == threadId) {
                        Iterator<Vertex> fromAddr = gtx.V()
                            .has("address", "address_hash", from);
                        Iterator<Vertex> toAddr = gtx.V()
                            .has("address", "address_hash", to);
                        if (fromAddr.hasNext() && toAddr.hasNext()) {
                            Vertex v1 = fromAddr.next();
                            Vertex v2 = toAddr.next();

                            if (g.V().has("address", "address_hash", from)
                                .outE("transaction")
                                .has("txn_hash", txnHash)
                                .hasNext()) {
                                skippedEdges++;
                                continue;
                            }

                            gtx.addE("transaction").from(v1).to(v2)
                                .property("txn_hash", txnHash)
                                .property("block_timestamp", blockTimestamp)
                                .property("txn_value", txnValue)
                                .dedup()
                                .iterate();
                            batchEdges++;
                        } else {
                            skippedEdges++;
                            // System.out.println("from addr " + from + " has vertex ? " + fromAddr.hasNext()
                            //     + " to addr " + to + " has vertex ? " + toAddr.hasNext());
                        }
                    }
                    if (batchEdges > PER_THREAD_BATCH_SIZE ) {
                        totalEdges += commitBatch(tx, threadId, batchEdges);
                        tx = g.tx();
                        gtx = tx.begin();
                        batchEdges = 0;
                    }
                }
                if (batchEdges > 0) {
                    totalEdges += commitBatch(tx, threadId, batchEdges);
                    batchEdges = 0;
                    tx = g.tx();
                    gtx = tx.begin();
                }
            } catch (Exception e) {
                System.out.println("Adding Transaction Edge exception : " + e.toString());
            } finally {
                if (batchEdges > 0) {
                    totalEdges += commitBatch(tx, threadId, batchEdges);
                }
                System.out.println("thread " + threadId + " finished uploading edgeds total " + totalEdges +
                    " skipped " + skippedEdges + " time " + (System.currentTimeMillis() - start));
            }
        }
    }
    public static void TestWithJanusGraphInMem() {
        // use janusgraph-inmemory.properties
        JanusGraph graph = JanusGraphFactory.open("conf/janusgraph-inmemory.properties");
        JanusGraphTransaction graphTransaction = graph.newTransaction();
        JanusGraphVertex v = graphTransaction.addVertex("root");
        v.property("balance", 100);
        graphTransaction = graph.newTransaction();
        v = graphTransaction.addVertex("child");
        v.property("balance", 200);
        graphTransaction.commit();

        graphTransaction = graph.newTransaction();
        GraphTraversalSource traversal = graphTransaction.traversal();
        Iterator<Vertex> root = traversal.V().has("root", "balance", 100);
        if (root.hasNext()) {
            System.out.println("traverse: root node " + root.toString());
        }
        root = traversal.V().has("root", "balance", 200);
        for (Iterator<Vertex> it = root; it.hasNext(); ) {
            System.out.println("traverse: vertex is " + it.next().toString());
        }
        graphTransaction.commit();
        graphTransaction.close();
        graph.close();
    }

    public static void main(String args[]) throws Exception {
        if (null == args || args.length < 3) {
            System.err.println(
                    "Usage: BulkLoader <janusgraph_host> " +
                        "<address.csv> or <transaction.csv>" + " <num_threads>");
            System.exit(1);
        }
        System.out.println("BulkLoader to JanusGraph ." + args[0]);

        String csv_file = args[1];
        int totalThreads = Integer.parseInt(args[2]);
        long start = System.currentTimeMillis();
        List<Thread> workers = new ArrayList<>();

        GraphTraversalSource g = initTraversal(args[0]);

        if (csv_file.contains("address")) {
            for (int i = 0; i < totalThreads; i++) {
                Thread t = new Thread(new AddressWorker(g, csv_file, i, totalThreads));
                workers.add(t);
                t.start();
            }
            for (Thread t : workers) {
                t.join();
            }
        } else if (csv_file.contains("transaction")) {
            for (int i = 0; i < totalThreads; i++) {
                Thread t = new Thread(new EdgeWorker(g, csv_file, i, totalThreads));
                workers.add(t);
                t.start();
            }
            for (Thread t : workers) {
                t.join();
            }
        } else {
            System.out.println("Need either address.csv or transaction.csv");
        }
        System.out.println("Loading address done ! Time: " + (System.currentTimeMillis() - start));
        System.exit(0);
    }
}
