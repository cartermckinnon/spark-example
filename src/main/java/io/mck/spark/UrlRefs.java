
package io.mck.spark;
/**
 A simple map-reduce job that turns 'outlinks' into 'inlinks' using Spark in Java

 By Carter McKinnon
 */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class UrlRefs
{
    /**
    
     @param args,
     Three arguments:
        master - Spark master ("local" or "spark-yarn")
        input - input file
        output - output directory
    
      @throws java.lang.Exception
      
     **/
    public static void main( String[] args ) throws Exception
    {
        // check arguments
        if( args.length != 3 )
        {
            System.err.println( "usage: UrlRefs master input output" );
            System.exit( 1 );
        }

        // create a Java Spark Context
        SparkConf conf = new SparkConf().setMaster( args[ 0 ] ).setAppName( "UrlRefs" );
        JavaSparkContext sc = new JavaSparkContext( conf );
        // Load input data.
        // space-delimited list of URLs: "www.a.com www.b.com www.c.com"
        // first URL represents a page, with the remaining URLs representing its outlinks
        sc.textFile( args[ 1 ] )
        .flatMapToPair(
        s ->
        {
            // output, list of tuples
            List<Tuple2<String, String>> pairs = new ArrayList<>();
            // URLs are space-delimited
            List<String> tokens = Arrays.asList( s.split( "\\s+" ) );
            // first URL is source page
            String source = tokens.get( 0 );
            // for each of its outlinks, map the source page as an inlink
            for( String link : tokens )
            {
                pairs.add( new Tuple2<>( link, source  ));
            }
            return pairs.iterator();
        })
        // gather inlinks for each outlink by concatenation
        .reduceByKey( (a, b)  -> a + " " + b )
        .saveAsTextFile( args[ 2 ] );
    }
}
