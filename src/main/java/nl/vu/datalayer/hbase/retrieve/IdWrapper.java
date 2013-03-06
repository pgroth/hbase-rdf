package nl.vu.datalayer.hbase.retrieve;

import nl.vu.datalayer.hbase.id.Id;

public class IdWrapper implements HBaseTripleElement {
	
	private Id id;

	public IdWrapper(Id id) {
		super();
		this.id = id;
	}

	public Id getId() {
		return id;
	}

	public void setId(Id id) {
		this.id = id;
	}

}
