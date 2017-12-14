package cassandra;


import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.util.ArrayList;
import java.util.List;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
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
	

	public void openSession() {
//		Session session = cluster.connect(keyspace);
		Session session = connector.openSession();
	}

	public void createDB() {
		
		Session createSession = cluster.connect();
		createSession.execute("CREATE KEYSPACE IF NOT EXISTS "+keyspace+" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
		createSession.execute("CREATE TABLE IF NOT EXISTS "+keyspace+"."+table+" (uuid TIMEUUID, hash INT, db TEXT, accession TEXT, description TEXT, amino TEXT,firstAminos TEXT, PRIMARY KEY((firstaminos, uuid)))");
//		createSession.execute("CREATE INDEX IF NOT EXISTS "+index+" ON "+keyspace+"."+table+" ("+index+");");
		createSession.close();
	}

	public void writeToCassandra(FastaObject object) {

		List<FastaObject> fastaToSave = new ArrayList<FastaObject>();
		JavaRDD<FastaObject> rdd2 = sparkContext.parallelize(fastaToSave);
  		javaFunctions(rdd2).writerBuilder(keyspace, table, mapToRow(FastaObject.class)).saveToCassandra();	


	}
	

	public boolean aminoLookup(FastaObject object) {
		ResultSet result = session.execute("SELECT * FROM "+table+" WHERE amino='"+object.getAmino()+"';");
		if (result.isExhausted()) {
//			System.out.println("NO AMINO MATCHES!");
			return false;
		}
//		System.out.println("AMINO EXISTS");
		return true;
	}
	
//	public boolean hashExist(FastaObject object) {
//		ResultSet result = session.execute("SELECT * FROM "+table+" WHERE hash="+object.getHash()+";");
//		if (result.isExhausted()) {
//			System.out.println("NO HASH MATCHES!");
//			return false;
//		}
//		System.out.println("HASH EXISTS");
//		return aminoExist(object, result);
//	}
	
//	public boolean firstAminosExist(FastaObject object) {
//		ResultSet result = session.execute("SELECT * FROM "+table+" WHERE firstAminos="+object.getFirstAminos()+";");
//		if (result.isExhausted()) {
//			System.out.println("NO FIRSTAMINOS MATCH!");
//			return false;
//		}
//		System.out.println("FIRSTAMINOS EXIST");
//		return aminoExist(object, result);
//	}
	
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
