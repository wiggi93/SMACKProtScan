package fastaParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import cassandra.CassandraConnection;
import models.FastaObject;


public class fastaParser {

	public static void main(String[] args) {


		File test = new File("C:\\Users\\Philip\\Desktop\\Arbeit\\fasta1k.fasta");
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
						object.setFirstAminos();
//						if (!cassandra.aminoLookup(object)){
						if (true) {
							countNew++;
							System.out.println("CountNew : "+countNew);
							System.out.println((liste.size()));
	//						cassandra.writeToCassandra(object);
						}
					}
					object= new FastaObject(temp, "");
					firstLine=false;
				} else {
					object.setAmino(object.getAmino() + temp.trim());
				}

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
//		cassandra.writeToCassandra(object);
//		cassandra.closeSession();
//		cassandra.closeCluster();
		System.out.println(System.currentTimeMillis()-time);
	}

}
