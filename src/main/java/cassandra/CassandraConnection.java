package cassandra;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.google.gson.Gson;

import fastaParser.SparkJob;
import models.FastaObject;

public class CassandraConnection implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static List<FastaObject> fastaList = new ArrayList<FastaObject>();
	private static JavaSparkContext sparkContext;
	private CassandraConnector connector;
	private static FastaObject test = new FastaObject("blaaablubb", "AOGIDGPOJGAEOEJOGDJGE");
	
	static String serverIP = "192.168.0.21";
	static String keyspace = "test";
	public String table	= "fasta";
	static String index = "amino";

	static Cluster cluster = Cluster.builder()
			.addContactPoints(serverIP)
			.build();
	
	
	public CassandraConnection(JavaSparkContext sc, CassandraConnector connector){
		sparkContext = sc;
		this.connector = connector;
	}

	public void createDB() {
		
		Session createSession = cluster.connect();
		createSession.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspace+" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
		createSession.execute("CREATE TABLE IF NOT EXISTS "+keyspace+"."+table+" (uuid TIMEUUID, db TEXT, accession TEXT, description TEXT, amino TEXT,firstaminos TEXT, randompartition INT, PRIMARY KEY((db,randompartition),amino,accession));");
//		createSession.execute("CREATE INDEX IF NOT EXISTS "+index+" ON "+keyspace+"."+table+" ("+index+");");
		createSession.close();
	}

	public void computeCassandraOperations(ConsumerRecord<String, String> rec){
		FastaObject fasta = new Gson().fromJson(rec.value(), FastaObject.class);
		fasta.setUuid(UUIDs.timeBased());
		fasta.setRandompartition((int)(Math.random()*5-1));
		fasta.setfirstaminos();
		System.out.println(test.toString());
		fastaList.add(test);
		writeListToCassandra();
	}
	
	private void writeListToCassandra() {
		JavaRDD<FastaObject> rdd = sparkContext.parallelize(fastaList);
  		javaFunctions(rdd).writerBuilder(keyspace, table, mapToRow(FastaObject.class)).saveToCassandra();	
	}

	public void closeCluster() {
		cluster.close();
	}

}
