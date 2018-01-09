package fastaParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.cql.CassandraConnector;

import cassandra.CassandraConnection;
import models.FastaObject;


public class fastaParser {

	public static JavaSparkContext sparkContext;
	
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("SMACKProtScan").setMaster("local[5]");
		sparkContext = new JavaSparkContext(conf);
		sparkContext.setLogLevel("ERROR");
		
		CassandraConnector connector = CassandraConnector.apply(conf);
        CassandraConnection cassandraConnection = new CassandraConnection(sparkContext, connector);
        cassandraConnection.createDB();
        
		File test = new File("/Users/philipwiegratz/Desktop/Workspace SpeL/fasta.fasta");
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
		int count = 0, countNew = 0;
		double time = System.currentTimeMillis();
		ArrayList<FastaObject> liste = new ArrayList<FastaObject>();
		
		try {
			while((temp=br.readLine())!=null) {
				if (temp.startsWith(">")) {
					if (!firstLine) {
						count++;
						System.out.println("Count : "+count);
//						if (!cassandra.aminoLookup(object)){
						if (true) {
							countNew++;
							System.out.println("CountNew : "+countNew);
							liste.add(object);
						}
					}
					object= new FastaObject(temp, "");
					firstLine=false;
				} else {
					object.setAmino(object.getAmino() + temp.trim());
				}
				if (liste.size()>=500) {
					cassandraConnection.writeListToCassandra(liste);
					liste.clear();
				}
				
				
			}
		
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		cassandraConnection.writeListToCassandra(liste);
		cassandraConnection.closeCluster();
		System.out.println(System.currentTimeMillis()-time);
	}

}
