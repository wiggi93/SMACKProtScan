package cassandra;


import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;

import models.FastaObject;

public class CassandraConnection {

	
	private static JavaSparkContext sparkContext;
	private CassandraConnector connector;
	
	static String serverIP = "127.0.0.1";
	static String keyspace = "test_proteomics";
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
		createSession.execute("CREATE TABLE IF NOT EXISTS "+keyspace+"."+table+" (uuid TIMEUUID, db TEXT, accession TEXT, description TEXT, amino TEXT,firstaminos TEXT, randompartition INT, PRIMARY KEY((db,randompartition),amino));");
//		createSession.execute("CREATE INDEX IF NOT EXISTS "+index+" ON "+keyspace+"."+table+" ("+index+");");
		createSession.close();
	}

	public void writeListToCassandra(ArrayList<FastaObject> liste) {
		JavaRDD<FastaObject> rdd = sparkContext.parallelize(liste);
  		javaFunctions(rdd).writerBuilder(keyspace, table, mapToRow(FastaObject.class)).saveToCassandra();	
	}
	
	public boolean aminoExist(FastaObject object, ResultSet result) {
		while(result.iterator().hasNext()) {
			if (object.getAmino().equals(result.iterator().next().getString("amino"))){
					System.out.println("AMINO EXIST");
					return true;
			}
		}
		return false;
	}

	public void closeCluster() {
		cluster.close();
	}

}
