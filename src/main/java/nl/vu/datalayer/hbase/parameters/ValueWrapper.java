package nl.vu.datalayer.hbase.parameters;


import org.openrdf.model.Value;

public class ValueWrapper implements HBaseTripleElement {
	
	private Value value;

	public ValueWrapper(Value value) {
		super();
		this.value = value;
	}

	public Value getValue() {
		return value;
	}

	public void setValue(Value value) {
		this.value = value;
	}

}
