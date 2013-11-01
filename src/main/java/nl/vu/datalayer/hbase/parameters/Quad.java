package nl.vu.datalayer.hbase.parameters;

import nl.vu.datalayer.hbase.id.Id;

public class Quad {
	
	public static final int S = 0;
	public static final int P = 1;
	public static final int O = 2;
	public static final int C = 3;

	private Id[] elems;

	public Quad(Id[] quad) {
		super();
		this.elems = quad;
	}

	public Id[] getElems() {
		return elems;
	}
	
}
