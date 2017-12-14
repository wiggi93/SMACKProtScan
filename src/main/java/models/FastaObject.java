package models;

public class FastaObject {

	public String db;
	public String accession;
	public String description;
	public String amino;
//	public int hash;
	public String firstAminos;

	public String getFirstAminos() {
		return firstAminos;
	}

	public void setFirstAminos() {
		if (this.amino.length()>=6)
			this.firstAminos = this.amino.substring(0, 6);
		else 
			this.firstAminos=this.amino.substring(0, this.amino.length());
		
	}
        
        public FastaObject(){
            this.accession=null;
            this.amino=null;
            this.db=null;
            this.description=null;
            this.firstAminos=null;
        }
	public FastaObject(String description, String amino) {
		int count=0;
		this.description=description;
		this.amino=amino;
		

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

//	public int getHash() {
//		return hash;
//	}
//
//	public void setHash(int hash) {
//		this.hash = hash;
//	}

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