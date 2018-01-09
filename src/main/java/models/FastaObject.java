package models;

import java.io.Serializable;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

public class FastaObject implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public UUID uuid;
	public String db;
	public String accession;
	public String description;
	public String amino;
	public String firstaminos;
	public int randompartition;

	
	public UUID getUuid() {
		return uuid;
	}

	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}
	
	public String getfirstaminos() {
		return firstaminos;
	}

	public void setfirstaminos() {
		if (this.amino.length()>=6)
			this.firstaminos = this.amino.substring(0, 6);
		else if (this.amino.length() > 0)
			this.firstaminos=this.amino.substring(0, this.amino.length()-1);
		else 
			this.firstaminos="-";
		
	}
        
        public FastaObject(){
            this.accession=null;
            this.amino=null;
            this.db=null;
            this.description=null;
            this.firstaminos=null;
            this.uuid=null;
            this.randompartition=(int)(Math.random()*4);
        }
	public int getRandompartition() {
			return randompartition;
		}

		public void setRandompartition(int randompartition) {
			this.randompartition = randompartition;
		}

	public FastaObject(String description, String amino) {
		int count=0;
		this.description=description;
		this.amino=amino;
		this.uuid = UUIDs.timeBased();
		setfirstaminos();
		this.randompartition=(int)(Math.random()*4);

		for (int i=0; i<description.length();i++) {
			if (description.charAt(i)=='|'){
				count++;
			}
		}	

		if (count>=2) {
			String[] parts1 = description.split("[|]");
			this.accession=parts1[1];
			String[] parts2 = parts1[0].split("[>]");
			this.db=parts2[1];
		}
		if (count==1) {
			String[] parts1 = description.split("[|]");
			String[] parts2 = parts1[0].split("[>]");
			this.accession=parts2[1];
			this.db="UNKNOWN";
		}
		if (count==0) {
			this.accession="UNKNOWN";
			this.db="UNKNOWN";
		}

	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getAccession() {
		return accession;
	}

	public void setAccession(String accession) {
		this.accession = accession;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getAmino() {
		return amino;
	}

	public void setAmino(String amino) {
		this.amino = amino;
	}

	@Override
	public String toString() {
		return "FastaObject [db=" + db + ", accession=" + accession + ", description=" + description + ", amino="
				+ amino + "]";
	}
}