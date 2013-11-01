package nl.vu.datalayer.hbase.parameters;

import java.util.ArrayList;

import nl.vu.datalayer.hbase.id.Id;

public class ResultColumn {
	
	private byte []colName;
	private ArrayList<Id> results;
	
	public ResultColumn(byte[] colName) {
		super();
		this.colName = colName;
		this.results = new ArrayList<Id>();
	}
	
	public void addResult(Id id){
		results.add(id);
	}

	public byte[] getColName() {
		return colName;
	}

	public ArrayList<Id> getResults() {
		return results;
	}

	public Id getResult(int index){
		return results.get(index);
	}	

}
