package nl.vu.datalayer.hbase.exceptions;

public class ElementNotFoundException extends Exception {
	
	private static final long serialVersionUID = -8998044204690506845L;

	public ElementNotFoundException() {
		super();
	}

	public ElementNotFoundException(String message) {
		super(message);
	}
}
