package io.mck.spark;

/**

    GraphX Single Source Shortest Path (SSSP) in Java using Pregel

    @author Carter McKinnon
 
 **/

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;


public class PregelSSSP
{
    /**
      @param args Command line arguments:
                  master - Spark master ("local" for single thread, "local[n]" for n threads, or "spark-yarn")
                  input - input file(s)
                  output - output directory
                  source - integer ID of source node for SSSP
     **/

    public static void main(String[] args)
    {
        // check args
        if (args.length != 4)
        {
            System.err.println("usage: PregelSSSP master input output source");
            System.exit(1);
        }

        // grab master
        String master = args[0];
        // grab input file(s)
        String _input = args[1];
        // grab output directory
        String output = args[2];
        // grab source node ID
        Long source = Long.parseLong(args[3]);

        // create a Java Spark Context
        SparkConf conf = new SparkConf().setMaster(master).setAppName("PregelSSSP");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // load input, one String per line
        // each line represents an edge of the graph: "sourceNodeID,destNodeID,edgeLength"
        JavaRDD<String> input = sc.textFile(_input);

        // create the edge objects
        JavaRDD<Edge<Long>> edges = input.map((s) ->
                                              {
                                                  String[] pieces = s.split(",");
                                                  return new Edge<>( Long.parseLong(pieces[0]),
                                                                     Long.parseLong(pieces[1]),
                                                                     Long.parseLong(pieces[2]) );
                                              });

        // construct a graph from the RDD of edges
        Graph just_edges = Graph.fromEdges( edges.rdd(),
                                            source,
                                            StorageLevel.MEMORY_ONLY(),
                                            StorageLevel.MEMORY_ONLY(),
                                            scala.reflect.ClassTag$.MODULE$.apply(Long.class),
                                            scala.reflect.ClassTag$.MODULE$.apply(Long.class) );

        // initialize vertices
        //  - set the source node's shortest-known distance to 0
        //  - set all other nodes' shortest-known distance to Long.MAX_VALUE (infinity)
        Graph graph = just_edges.mapVertices( new VERTEX_INIT(),
                                              scala.reflect.ClassTag$.MODULE$.apply(Long.class),
                                              null );
        graph.persist(StorageLevel.MEMORY_ONLY());

        // find the single shortest path from source
        System.out.println("Running Pregel with " + graph.edges().count() + " edges and " + graph.vertices().count() + " vertices");
        
        Graph sssp = graph.ops().pregel( Long.MAX_VALUE,         // initial message
                                         Integer.MAX_VALUE,      // maximum number of iterations
                                         EdgeDirection.Out(),    // which vertices must be active for send
                                         new RECEIVE_MESSAGE(),  // receive message at a vertex
                                         new SEND_MESSAGES(),    // send messages on an edge
                                         new MERGE_MESSAGES(),   // merge two messages at a vertex
                                         scala.reflect.ClassTag$.MODULE$.apply(Long.class ));
        
        System.out.println("Pregel complete. Saving to '" + output + '\'' );

        // save vertices to output directory
        sssp.vertices().saveAsTextFile(output);
        
        System.out.println("Done.");
    }

    /**
     * VertexInit - initialize vertex data
     *
     * Source node initialized to 0 (its distance from source is 0)
     * Other nodes initialized to infinity/Long.MAX_VALUE (their distance from source is unknown)
     * 
     * All vertices were loaded with the source node's ID as their vertex data,
     * so the source node's ID is 'oldData' below.
     */
    static class VERTEX_INIT
    extends AbstractFunction2<Long, Long, Long>
    implements Serializable
    {
        @Override
        public Long apply(Long vertexID, Long oldData)
        {
            if( vertexID.equals(oldData) )
            {
                System.out.println("Found source node");
                return 0L;
            }
            return Long.MAX_VALUE;
        }
    };

    /**
     * ReceiveMessage - process a received message at a vertex
     *
     * Inputs: vertex ID, vertex data, message
     * Output: new vertex data
     */
    static class RECEIVE_MESSAGE
    extends AbstractFunction3<Long, Long, Long, Long>
    implements Serializable
    {
        @Override
        public Long apply( Long vertexID, Long vertexData, Long message )
        {
            // If incoming distance is < our shortest-known distance, save incoming value
            if( message < vertexData )
            {
                return message;
            }
            return vertexData;
        }
    }

    /**
     * SendMessages - send any messages for an EdgeTriplet
     *
     * Input: EdgeTriplet (vertices and values, edge value)
     * Output: iterator of messages to send
     *          (each message is destination and message value)
     */
    static class SEND_MESSAGES
    extends AbstractFunction1<EdgeTriplet<Long, Long>, Iterator<Tuple2<Long, Long>>>
    implements Serializable
    {
        @Override
        public Iterator<Tuple2<Long, Long>> apply( EdgeTriplet<Long, Long> edge )
        {
            // output, a list of messages
            List<Tuple2<Long,Long>> msgs = new ArrayList<>();
            // If distance of source vertex + edge cost is smaller than distance of destination...
            if( edge.srcAttr() != Long.MAX_VALUE
                   &&
                edge.srcAttr() + edge.attr() < edge.dstAttr() )
            {
                // Send source's distance + edge cost to destination
                msgs.add(new Tuple2<>(edge.dstId(), edge.srcAttr() + edge.attr()));
            }
            // Return iterator over messages to send (must be a Scala iterator)
            return JavaConverters.asScalaIteratorConverter(msgs.iterator()).asScala();
        }
    }
    
    /**
     * MergeMessages - reduce the value of two messages to a single value.
     * 
     * Input: Two messages, representing distances to the source node
     * Output: The shorter of the two distances
     */
    static class MERGE_MESSAGES
    extends AbstractFunction2<Long, Long, Long>
    implements Serializable
    {
        @Override
        public Long apply( Long msg1, Long msg2 )
        {
            return (msg1 <= msg2) ? msg1 : msg2;
        }
    }
}

