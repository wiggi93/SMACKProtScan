package fastaParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.datastax.spark.connector.cql.CassandraConnector;

import cassandra.CassandraConnection;
import models.FastaObject;


public class fastaParser {

	public static SparkContext sparkContext;
	public static JavaSparkContext javaSparkContext;
	public static int partitionCount = 1000;
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SMACKProtScan");
//		SparkSession session = SparkSession.builder()
//				.appName("SMACKProtScan")
//				.config(conf)
//				.getOrCreate();
//		
//		sparkContext = session.sparkContext();
//		javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
		
		sparkContext = new SparkContext(conf);
		javaSparkContext = new JavaSparkContext(sparkContext);
		javaSparkContext.setLogLevel("ERROR");
		
//		CassandraConnector connector = CassandraConnector.apply(conf);
//        CassandraConnection cassandraConnection = new CassandraConnection(javaSparkContext, connector);
//        cassandraConnection.createDB();
        
		File test = new File("testfasta.fasta");
        
//        File test = new File("/Users/philipwiegratz/Desktop/WorkspaceSpeL/MG_BG_BielefeldAggregated.fasta");
        
		FileReader fr = null;
		try {
			fr = new FileReader(test);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		BufferedReader br = new BufferedReader(fr);
		String temp= "";
		FastaObject object=null;
		boolean firstLine=true;
		int count = 0;
		double time = System.currentTimeMillis();
		ArrayList<FastaObject> liste = new ArrayList<FastaObject>();
		
		try {
			while((temp=br.readLine())!=null) {
				if (temp.startsWith(">")) {
					if (!firstLine) {
						count++;
						System.out.println("Count : "+count);
						liste.add(object);
					}
					object= new FastaObject(temp, "");
					firstLine=false;
				} else {
					object.setAmino(object.getAmino() + temp.trim());
				}
				if (liste.size()>=500) {
//					cassandraConnection.writeListToCassandra(liste);
					liste.clear();
				}
			}
		liste.add(object);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
//		cassandraConnection.writeListToCassandra(liste);
//		cassandraConnection.closeCluster();
		System.out.println(System.currentTimeMillis()-time);
	}

}
