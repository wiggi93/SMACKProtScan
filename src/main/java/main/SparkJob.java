package main;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.datastax.spark.connector.cql.CassandraConnector;

import cassandra.CassandraConnection;


public class SparkJob {

    public static JavaSparkContext sparkContext;
    public final static String CASSANDRA_URL = "localhost";//64

	
    public static void main(String[] args) {

	SparkConf conf = new SparkConf().setAppName("speckvonschmeck").setMaster("local[4]");
		
		sparkContext = new JavaSparkContext(conf);
		sparkContext.setLogLevel("ERROR");
		JavaStreamingContext context = new JavaStreamingContext(sparkContext, new Duration(2000));
		 
        CassandraConnector connector = CassandraConnector.apply(conf);
        CassandraConnection cassandraConnection = new CassandraConnection(sparkContext, connector);
        cassandraConnection.createDB();     
        
    }
}
