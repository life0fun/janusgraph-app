import java.io.File
import java.io.FileReader
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.net.HttpURLConnection
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService

import org.janusgraph.core.JanusGraph
import org.janusgraph.core.JanusGraphFactory
import org.janusgraph.core.JanusGraphTransaction
import org.janusgraph.core.schema.JanusGraphManagement
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
// gremlin-driver module
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
// gremlin-core module
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

executorService = java.util.concurrent.Executors.newFixedThreadPool(4)

// bin/gremlin.sh -e path/to/your/script.groovy
// Define your JanusGraph server details
// Create a new Cluster object with the server details
//g = traversal().withRemote(
//        DriverRemoteConnection.using('localhost', 8182))
//println("total nodes " + g.V().count() + " graph " + g.getGraph().toString())

def loadNodesSingleThread(org.janusgraph.core.JanusGraph graph, csv_file, thrdId, totalThreads) {
  def chunkSize = 1024
//  def csvFile = new File("/opt/scripts/small_test_address.csv")
  def csvFile = new File(csv_file)
  def reader
  def lineNo = 0;
  def line
  def batch_nodes = 0
  def total_nodes = 0
  def skipped_nodes = 0
  def start = System.currentTimeMillis()
  try {
    reader = new BufferedReader(new FileReader(csvFile))
    println "Thread $thrdId Openning file " + csvFile.toPath()
    while (true) {
      line = reader.readLine()
      if (line == null) {
        break
      }
      lineNo++
      if ((int) ((int)((lineNo - 1) / chunkSize) % totalThreads) == thrdId) {
        def parts = line.split(',')
        def address = parts[0]
        def balance = parts.size() > 1 ? parts[1] : null
        def props = parts.size() > 2 ? parts[2] : null

        def existingVertex = graph.traversal().V().has('address', 'address_hash', address).tryNext()
        if (existingVertex.isPresent()) {
          //println("Skipping duplicate entry for address: ${address}")
          skipped_nodes++
          continue
        }
        def v = graph.addVertex(T.label, 'address', 'address_hash', address)
        if (balance) {
          v.property('balance', Double.parseDouble(balance))
        }
        if (props) {
          v.property('props', props)
        }
        batch_nodes++
        // println "thread id $thrdId upload lineNo $lineNo " + line
        continue
      }
      if (batch_nodes > 0) {
        total_nodes += batch_nodes
        graph.tx().commit()
        println "thread id $thrdId commit batch $batch_nodes nodes"
        batch_nodes = 0
      }
    }
  } catch (e) {
    println "loadNodesSingleThread encounter error: " + e.toString()
  } finally {
    if (batch_nodes > 0) {
      total_nodes += batch_nodes
      graph.tx().commit()
    }
    println("thread id $thrdId commit total $total_nodes nodes, " +
            "skipped $skipped_nodes time: " + (System.currentTimeMillis() - start))
    reader.close()
  }
}

def loadNodesRunner(graph, csv_file, tid, totalThreads) {
  return { -> loadNodesSingleThread(graph, csv_file, tid, totalThreads) } as Runnable
}

def loadNodesThreads(graph, csv_file) {
  def totalThreads = 4
//  def executorService = Executors.newFixedThreadPool(totalThreads)
  def threads = []
  for (int i = 0; i < totalThreads; i++) {
    threads[i] = new Thread(loadNodesRunner(graph, csv_file, i, totalThreads))
    threads[i].start()
  }
  for (int i = 0; i < totalThreads; i++) {
    threads[i].join()
    println "thread $i finished "
  }
}


