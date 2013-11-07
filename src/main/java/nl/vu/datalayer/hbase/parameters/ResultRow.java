package nl.vu.datalayer.hbase.parameters;

import java.util.ArrayList;

import nl.vu.datalayer.hbase.id.Id;

public class ResultRow extends ArrayList<Id>{

	public ResultRow(ArrayList<Id> ids) {
		super(ids);
	}
	
	public ResultRow(int capacity){
		super(capacity);
	}
	
	public ResultRow(ResultRow row) {
		super(row.size());
		for (Id id : row) {
			this.add(id);
		}
	}

	public ResultRow() {
		super();
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -8733255678366288671L;
	
}
